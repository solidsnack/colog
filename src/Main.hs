{-# LANGUAGE OverloadedStrings, BangPatterns, PatternGuards,
             ScopedTypeVariables, FlexibleInstances, DeriveDataTypeable #-}

module Main where

import qualified Aws
import qualified Aws.S3 as S3

import qualified Data.Attoparsec.Text as Atto
import           Control.Applicative
import           Control.Concurrent ( threadDelay )
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import qualified Control.Concurrent.MSem as Sem
import           Control.Concurrent.STM
import           Control.Monad ( when, forM, unless )
import           Control.Monad.IO.Class
import           Control.Exception ( catch, throwIO, Exception(..), SomeException )
import qualified Data.ByteString.Char8 as B
import           Data.Conduit ( runResourceT, ResourceT, ($$+-), (=$) )
import           Data.Conduit.BZlib ( bunzip2 )
import           Data.Maybe ( fromMaybe, catMaybes )
import           Data.Monoid
import qualified Data.Heap as Heap
import           Data.String ( fromString )
import qualified Data.Text as T
import           Data.Time.Clock ( getCurrentTime )
import           Data.Typeable ( Typeable(..) )
import           Network.HTTP.Conduit ( withManager, Manager, responseHeaders,
                                        HttpException(..), responseBody )
import           Network.HTTP.Types.Header ( hContentType )
import           System.Console.CmdTheLine
import           System.Environment
import           System.Exit ( exitFailure )
import           System.IO
import           System.IO.Streams ( InputStream, Generator, yield )
import qualified System.IO.Streams as Streams
import           System.CPUTime
import qualified Text.PrettyPrint as Pretty

import           System.Log.Colog.DateMatch
import           System.Log.Colog.LineFilter

data Config = Config 
  { cfgAwsCfg     :: !Aws.Configuration
  , cfgS3Cfg      :: !(S3.S3Configuration Aws.NormalQuery)
  , cfgLoggerLock :: !(MVar ())
  , cfgManager    :: !Manager
  , cfgBucketName :: !T.Text
  , cfgReadFileThrottle :: !(Sem.MSem Int)
  , cfgLogLevel   :: !LogLevel
  , cfgAppendInstanceId :: !Bool
  }

type LogLevel = Int

data Range = Range !DatePrefix !DatePrefix
  deriving (Show)

msg :: Config -> LogLevel -> String -> IO ()
msg Config{ cfgLoggerLock = lock, cfgLogLevel = logLevel } level text =
 if level > logLevel then return () else
   withMVar lock (\_ -> hPutStrLn stderr text)

debugMessage :: Config -> String -> IO ()
debugMessage cfg text = msg cfg 2 text

defaultMaxKeys :: Maybe Int
defaultMaxKeys = Nothing

bucketParser :: Atto.Parser (T.Text, T.Text)
bucketParser = do
  _ <- Atto.string "s3://"
  bucketName <- Atto.takeWhile (/= '/')
  _ <- Atto.char '/'
  rootPath <- Atto.takeText
  return (bucketName, rootPath)

term :: Term (IO ())
term = program <$> required (serverArg 0)
               <*> required (patternArg 1)
               <*> value (flag optDebug)
               <*> value (flag optAppendInstanceId)
 where
   optDebug = (optInfo ["debug"])
     { optDoc = "Turn on debug output." }
   optAppendInstanceId = (optInfo ["append-instance-id"])
     { optDoc = "Append the server's instance id to each log entry line." }

data LogRoot = LogRoot !T.Text !T.Text
  deriving Show

instance ArgVal LogRoot where
  converter = (argParser, argPrinter)
   where
     argParser str =
       case Atto.parseOnly bucketParser (T.pack str) of
         Right (bucket, path) -> Right (LogRoot bucket path)
         _ -> Left "Invalid log server format"
     argPrinter (LogRoot bucket path) =
       "s3://" <> fromString (T.unpack bucket) <>
       "/" <> fromString (T.unpack path)

instance ArgVal (Maybe LogRoot) where converter = just

serverArg :: Int -> Arg (Maybe LogRoot)
serverArg n = pos n Nothing posInfo
                { posName = "SERVER"
                , posDoc = "s3://BUCKET/ROOTDIR/"
                }

data RangePattern = RangePattern !T.Text
  deriving Show

instance ArgVal RangePattern where
  converter = (argParser, argPrinter)
   where
     argParser str = Right (RangePattern (T.pack str))
     argPrinter (RangePattern str) = fromString (T.unpack str)

instance ArgVal (Maybe RangePattern) where converter = just

patternArg :: Int -> Arg (Maybe RangePattern)
patternArg n = pos n Nothing posInfo
                 { posName = "STARTDATE[/ENDDATE]"
                 , posDoc = "Start and optional end date of the logs to be " <>
                            "requested.  A date can be any valid prefix of " <>
                            "an ISO 8601 date.  The pattern \"...\" can be " <>
                            "used to mean any date.  If the end date is " <>
                            "missing then start date will be used in its " <>
                            "place.\n\nExample: 2013-03-01T12:34"
                 }

main :: IO ()
main = run (term, defTI{ termName = colog
                       , termDoc = doc
                       , man = extraMan})
 where
   colog = "colog"
   doc = "Aggregate and combine logs from several servers."
   extraMan =
     [ S "EXAMPLES"
     , P "Note that all dates and times are in UTC time."
     , I "List all logs from March 1st, 2013:"
         (colog ++ " s3://mybucket/logs/ 2013-03-01")
     , I "List all logs from March 1st, 2013, 14:15 to 14:30 (inclusive):"
         (colog ++ " s3://mybucket/logs/ 2013-03-01T14:15/2013-03-01T14:30")
     , I "List all logs from March 23st, 2013 until today:"
         (colog ++ " s3://mybucket/logs/ 2013-03-23/...")
     , I "List all logs before and including January 11st, 2013:"
         (colog ++ " s3://mybucket/logs/ .../2013-01-11")
     , I "List all logs:"
         (colog ++ " s3://mybucket/logs/ ...")
     ]

para :: String -> Pretty.Doc
para = Pretty.fsep . map Pretty.text . words

err :: Pretty.Doc -> IO a
err doc = hPutStrLn stderr (Pretty.render doc) >> exitFailure

expectJustOrFail :: Maybe a -> Pretty.Doc -> IO a
expectJustOrFail Nothing errMsg = err errMsg
expectJustOrFail (Just x) _     = return x

program :: LogRoot -> RangePattern -> Bool -> Bool -> IO ()
program (LogRoot bucketName rootPath)
        (RangePattern rangeText)
        debug appendInstanceId = do
  access_key <- fromMaybe "" <$> lookupEnv "AWS_ACCESS_KEY"
  secret_key <- fromMaybe "" <$> lookupEnv "AWS_SECRET_KEY"

  when (any null [access_key, secret_key]) $ do
    err $ para $
      "ERROR: Access key or secret key missing. Ensure that environment " ++
      "variables AWS_ACCESS_KEY and AWS_SECRET_KEY are defined."

  now <- getCurrentTime
  (fromDate, toDate)
     <- parseDateRange now rangeText `expectJustOrFail` (para $
          "Could not parse date range. Invoke program with --help for " ++
          "info on supported syntax.")

  loggerLock <- newMVar ()
  throttle <- Sem.new 20

  withManager $ \mgr -> liftIO $ (do

    let awsCfg = Aws.Configuration
                  { Aws.timeInfo = Aws.Timestamp
                  , Aws.credentials =
                      Aws.Credentials (B.pack access_key)
                                      (B.pack secret_key)
                  , Aws.logger = \lvl text -> 
                                   if lvl < Aws.Warning then return () else
                                     debugMessage cfg (T.unpack text)
                                 {- Aws.defaultLog Aws.Debug -} }

        s3cfg = Aws.defServiceConfig :: S3.S3Configuration Aws.NormalQuery

        cfg = Config { cfgAwsCfg = awsCfg
                     , cfgS3Cfg = s3cfg
                     , cfgLoggerLock = loggerLock
                     , cfgManager = mgr
                     , cfgBucketName = bucketName
                     , cfgReadFileThrottle = throttle
                     , cfgLogLevel = if debug then 2 else 0
                     , cfgAppendInstanceId = appendInstanceId
                     }

    let range = Range fromDate toDate
    debugMessage cfg (show range)

    all_servers <- Streams.toList =<< lsS3 cfg rootPath
    let !servers = {- take 10 -} all_servers

    queues <- forM servers $ \server -> do
                q <- newTBQueueIO 10
                worker <- async (processServer cfg server range q)
                link worker
                return (server, q, worker)

    grabAndDistributeFiles cfg queues)
      `catch` \(e :: SomeException) -> do
        withMVar loggerLock (\_ -> hPutStrLn stderr (show e))
        exitFailure

type ServerPath = S3Path

type FileNameProducer = (ServerPath, TBQueue (Maybe S3ObjectKey), Async ())

nextAvailableMinutes :: [FileNameProducer]
                     -> IO [(S3ObjectKey, FileNameProducer)]
nextAvailableMinutes [] = return []
nextAvailableMinutes producers = do
  mb_pairs
    <- forM producers $ \ producer@(_,q,_worker) -> do
         mb_minute <- atomically (peekTBQueue q)
         case mb_minute of
           Nothing -> return Nothing
           Just minute -> return (Just (minute, producer))
  return (catMaybes mb_pairs)

startMinuteFileReaders :: Config
                       -> S3ObjectKey
                       -> [(S3ObjectKey, FileNameProducer)]
                       -> IO [LineProducer]
startMinuteFileReaders cfg nextMinute minutes = do
  lineProducers
    <- forM minutes $ \(minute, (server, q, _worker)) -> do
         if minute /= nextMinute then return Nothing else do
           _ <- atomically (readTBQueue q)
           lineVar <- newLineBuffer
           producer <- async (processLines cfg (server <> minute) lineVar)
           link producer  -- make sure we fail if the producer files
           return (Just (producer, lineVar))
  return (catMaybes lineProducers)

grabAndDistributeFiles :: Config -> [FileNameProducer] -> IO ()
grabAndDistributeFiles _ [] = return ()
grabAndDistributeFiles cfg producers0 = do
    queue <- newTBQueueIO 2
    worker <- async (processLineWorker queue)
    link worker
    go queue producers0
    atomically (writeTBQueue queue Nothing)
    wait worker
 where
   go :: TBQueue (Maybe (IO ())) -> [FileNameProducer] -> IO ()
   go _queue [] = return ()
   go queue producers = do
     minutes <- nextAvailableMinutes producers
     unless (null minutes) $ do
       let !nextMinute = minimum (map fst minutes)
       lineProducers <- startMinuteFileReaders cfg nextMinute minutes

       atomically $ writeTBQueue queue $ Just $ do
         debugMessage cfg ("OUT: " ++ T.unpack nextMinute)
         t <- getCPUTime
         processAllLines cfg lineProducers B.putStr
         t' <- getCPUTime
         debugMessage cfg ("DONE: " ++ T.unpack nextMinute ++ " "
                ++ show (fromIntegral (t' - t) / 1000000000.0 :: Double)
                ++ " ms")

       go queue (map snd minutes)

   processLineWorker queue = do
     mb_act <- atomically (readTBQueue queue)
     case mb_act of
       Nothing -> return ()
       Just act -> do
         _ <- act
         processLineWorker queue

processServer :: Config -> S3Path -> Range -> TBQueue (Maybe S3ObjectKey)
              -> IO ()
processServer cfg serverPath range queue = do
  let !srvLen = T.length serverPath
  keys <- lsObjects cfg serverPath range
  let writeOne key = do
          let !file = T.drop srvLen key
          atomically (writeTBQueue queue (Just file))
  Streams.skipToEof =<< Streams.mapM_ writeOne keys
  atomically (writeTBQueue queue Nothing)

type Line = B.ByteString
type S3Path = T.Text
type S3ObjectKey = T.Text

lsS3 :: Config -> T.Text -> IO (InputStream T.Text)
lsS3 cfg path = Streams.fromGenerator (chunkedGen (lsS3_ cfg path))

-- | Return a stream of minute file names matching the given date
-- range.  Files are returned in ASCII-betical order.
--
-- The resulting stream is not thread-safe.
lsObjects :: Config
          -> S3Path -- ^ Absolute path of server's log directory
          -> Range  -- ^ Range requested
          -> IO (InputStream S3ObjectKey)
lsObjects cfg serverPath (Range startDate endDate) = do
  keys <- Streams.fromGenerator
    (chunkedGen (matching_ cfg serverPath startDate))
  let !srvLen = T.length serverPath
  let inRange path = isBefore endDate (Date (T.drop srvLen path))
  takeWhileStream inRange keys

type LineBuffer a = TBQueue a  --TMVar a

newLineBuffer :: IO (LineBuffer a)
newLineBuffer = newTBQueueIO 100 --newEmptyTMVarIO

pushLine :: LineBuffer a -> a -> STM ()
pushLine = writeTBQueue --putTMVar

popLine :: LineBuffer a -> STM a
popLine = readTBQueue  --takeTMVar

bzip2ContentType :: B.ByteString
bzip2ContentType = B.pack "application/x-bzip2"

data UnsupportedContentType = UnsupportedContentType !S3Path !B.ByteString
  deriving (Typeable)

instance Exception UnsupportedContentType

instance Show UnsupportedContentType where
  show (UnsupportedContentType file ty) =
    "File " ++ show file ++ " uses an unsupported content type: " ++ show ty

processLines :: Config -> S3Path -> LineBuffer (Maybe Line) -> IO ()
processLines cfg objectPath output = do
  let !obj = B.singleton ',' `B.append` B.pack (T.unpack objectPath) `B.snoc` '\n'
      !appendId = cfgAppendInstanceId cfg
  runRequest cfg $ do
    --liftIO $ debugMessage cfg ("OBJECT: " ++ show objectPath)
    t <- liftIO $ getCPUTime
    rsp <- Aws.pureAws (cfgAwsCfg cfg) (cfgS3Cfg cfg)
                       (cfgManager cfg) $!
             S3.getObject (cfgBucketName cfg) objectPath
    let writeLine line = do
          let !line' | appendId  = B.init line `B.append` obj
                     | otherwise = line
          liftIO (atomically (pushLine output (Just line')))
    t' <- liftIO $ getCPUTime
    liftIO $ debugMessage cfg ("GOT_OBJECT: " ++ show objectPath ++ " "
           ++ show (fromIntegral (t' - t) / 1000000000.0 :: Double) ++ " ms")
    case lookup hContentType (responseHeaders (S3.gorResponse rsp)) of
      Nothing -> return ()
      Just enc ->
        if enc /= bzip2ContentType then do
          liftIO $ debugMessage cfg $ "Unsupported content type: " ++ show enc
          liftIO $ throwIO $ UnsupportedContentType objectPath enc
         else return ()
    responseBody (S3.gorResponse rsp) $$+-
      (bunzip2 =$
       csvLines =$
       sortWithWindow 500 =$
       csvLineSink (\l -> writeLine l >> return True))
    liftIO (atomically (pushLine output Nothing))

type LineProducer = (Async (), LineBuffer (Maybe Line))

data SortByFirst a b = SBF !a !b

instance Eq a => Eq (SortByFirst a b) where
  SBF a1 _ == SBF a2 _ = a1 == a2

instance Ord a => Ord (SortByFirst a b) where
  SBF a1 _ `compare` SBF a2 _ = a1 `compare` a2

type LineHeap = Heap.Heap (SortByFirst Line LineProducer)

processAllLines :: Config -> [LineProducer] -> (Line -> IO ()) -> IO ()
processAllLines _cfg producers0 kont =
   fillHeap Heap.empty producers0
 where
   grabNextLine :: LineProducer -> LineHeap -> IO LineHeap
   grabNextLine producer@(_process, var) !heap = do
     next <- atomically (popLine var)
     case next of
       Nothing -> do
         return heap
       Just line -> do
         let !heap' = Heap.insert (SBF line producer) heap
         return heap'

   fillHeap :: LineHeap -> [LineProducer] -> IO ()
   fillHeap !heap (producer : producers) = do
     heap' <- grabNextLine producer heap
     fillHeap heap' producers
   fillHeap !heap [] = consumeHeap heap

   consumeHeap :: LineHeap -> IO ()
   consumeHeap !heap =
     case Heap.uncons heap of
       Nothing -> return ()
       Just (SBF line producer, heap') -> do
         kont line
         heap'' <- grabNextLine producer heap'
         consumeHeap heap''

--- Internals ----------------------------------------------------------

data Response a
  = Done
  | Full !a
  | More !a (IO (Response a))

runRequest :: Config -> ResourceT IO a -> IO a
runRequest cfg act0 = withRetries 8 5 act0
 where
   withRetries :: Int -> Int -> ResourceT IO a -> IO a
   withRetries !n !delayInMS act =
     runResourceT act `catch` (\(e :: HttpException) -> do
                    if n > 0 then do
                       debugMessage cfg $ "HTTP-Error: " ++ show e ++ "\nRetrying in "
                                  ++ show delayInMS ++ "ms..."
                       threadDelay (delayInMS * 1000)
                       withRetries (n - 1) (delayInMS + delayInMS) act
                      else do
                        debugMessage cfg $ "HTTP-Error: " ++ show e
                                   ++ "\nFATAL: Giving up"
                        throwIO e)

takeWhileStream :: (a -> Bool) -> InputStream a -> IO (InputStream a)
takeWhileStream predicate input = Streams.fromGenerator go
 where
   go = do mb_a <- liftIO (Streams.read input)
           case mb_a of
             Nothing -> return ()
             Just a -> if predicate a then yield a >> go else return ()

lsS3_ :: Config -> S3Path -> IO (Response [S3Path])
lsS3_ cfg path = go Nothing
 where
   go marker = do
      runRequest cfg $ do
        liftIO $ debugMessage cfg ("REQ: " ++ show path ++
                          maybe "" ((" FROM: "++) . show) marker)
        rsp <- Aws.pureAws (cfgAwsCfg cfg) (cfgS3Cfg cfg)
                           (cfgManager cfg) $!
                 S3.GetBucket{ S3.gbBucket = cfgBucketName cfg
                             , S3.gbPrefix = Just path
                             , S3.gbDelimiter = Just "/"
                             , S3.gbMaxKeys = defaultMaxKeys
                             , S3.gbMarker = marker
                             }
        --liftIO (print rsp)
        case S3.gbrCommonPrefixes rsp of
          [] -> return Done
          entries
           | Just maxKeys <- S3.gbrMaxKeys rsp , length entries < maxKeys
           -> return (Full entries)
           | otherwise
           -> do
             let !marker' = last entries
             return $! More entries (go $! Just marker')

matching_ :: Config -> S3Path -> DatePrefix -> IO (Response [S3ObjectKey])
matching_ cfg serverPath fromDate =
  go (Just (serverPath <> toMarker fromDate))
 where
   go marker = Sem.with (cfgReadFileThrottle cfg) $ do
     runRequest cfg $ do
       liftIO $ debugMessage cfg ("REQ: " ++ show serverPath ++
                          maybe "" ((" FROM: "++) . show) marker)
       rsp <- Aws.pureAws (cfgAwsCfg cfg) (cfgS3Cfg cfg)
                           (cfgManager cfg) $!
                 S3.GetBucket{ S3.gbBucket = cfgBucketName cfg
                             , S3.gbPrefix = Just serverPath
                             , S3.gbDelimiter = Nothing
                             , S3.gbMaxKeys = defaultMaxKeys
                             , S3.gbMarker = marker
                             }
       case map S3.objectKey (S3.gbrContents rsp) of -- REVIEW: Data.Vector?
         [] -> return Done
         entries
          | Just maxKeys <- S3.gbrMaxKeys rsp , length entries < maxKeys
          -> return (Full entries)
          | otherwise
          -> do
            let !marker' = last entries
            return $! More entries (go $! Just marker')

chunkedGen :: IO (Response [a]) -> Generator a ()
chunkedGen action0 = go action0
 where
   go action = do
     mb_entries <- liftIO action
     case mb_entries of
       Done -> return ()
       Full entries ->
         mapM_ yield entries
       More entries action' -> do
         mapM_ yield entries
         go action'
