{-# LANGUAGE OverloadedStrings, BangPatterns, PatternGuards,
             ScopedTypeVariables #-}

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
import           Control.Exception ( catch, throwIO )
import qualified Data.ByteString.Char8 as B
import           Data.Conduit ( runResourceT, ResourceT, ($$+-), (=$) )
import           Data.Conduit.BZlib ( bunzip2 )
import           Data.Maybe ( fromMaybe, catMaybes )
import           Data.Monoid
import qualified Data.Heap as Heap
import qualified Data.Text as T
import           Network.HTTP.Conduit ( withManager, Manager,
                                        HttpException(..), responseBody )
import           System.Environment
import           System.Exit ( exitFailure )
import           System.IO
import           System.IO.Streams ( InputStream, Generator, yield )
import qualified System.IO.Streams as Streams
import           System.CPUTime

import           DateMatch
import           LineFilter

data Config = Config 
  { cfgAwsCfg     :: !Aws.Configuration
  , cfgS3Cfg      :: !(S3.S3Configuration Aws.NormalQuery)
  , cfgLoggerLock :: !(MVar ())
  , cfgStdoutLock :: !(MVar (Async ()))
  , cfgManager    :: !Manager
  , cfgBucketName :: !T.Text
  , cfgReadFileThrottle :: !(Sem.MSem Int)
  }

data Range = Range !DatePattern !DatePattern

msg :: Config -> String -> IO ()
msg Config{ cfgLoggerLock = lock } text =
  withMVar lock (\_ -> hPutStrLn stderr text)

defaultMaxKeys :: Maybe Int
defaultMaxKeys = Nothing

printUsage :: IO ()
printUsage = do
    hPutStrLn stderr $
      "USAGE: colog s3://BUCKETNAME/LOGROOT/ FROMDATE[/TODATE]"

bucketParser :: Atto.Parser (T.Text, T.Text)
bucketParser = do
  _ <- Atto.string "s3://"
  bucketName <- Atto.takeWhile (/= '/')
  _ <- Atto.char '/'
  rootPath <- Atto.takeText
  return (bucketName, rootPath)


main :: IO ()
main = do
  access_key <- fromMaybe "" <$> lookupEnv "AWS_ACCESS_KEY"
  secret_key <- fromMaybe "" <$> lookupEnv "AWS_SECRET_KEY"

  args <- getArgs
  when (length args < 3) $ do

  bucketNameAndrootPath : otherArgs <- getArgs

  (bucketName, rootPath)
     <- case (Atto.parseOnly bucketParser (T.pack bucketNameAndrootPath)) of
          Right (b, r) -> return (b, r)
          Left _msg -> do
            printUsage
            error "Failed to parse bucket"

  when (any null [access_key, secret_key]) $ do
    putStrLn $ "ERROR: Access key or secret key missing\n" ++
               "Ensure that environment variables AWS_ACCESS_KEY and " ++
               "AWS_SECRET_KEY\nare defined"
    exitFailure

  loggerLock <- newMVar ()
  stdoutLock <- newMVar =<< async (return ())
  throttle <- Sem.new 20

  withManager $ \mgr -> liftIO $ do

    let awsCfg = Aws.Configuration
                  { Aws.timeInfo = Aws.Timestamp
                  , Aws.credentials =
                      Aws.Credentials (B.pack access_key)
                                      (B.pack secret_key)
                  , Aws.logger = \lvl text -> 
                                   if lvl < Aws.Warning then return () else
                                     msg cfg (T.unpack text)
                                 {- Aws.defaultLog Aws.Debug -} }

        s3cfg = Aws.defServiceConfig :: S3.S3Configuration Aws.NormalQuery

        cfg = Config { cfgAwsCfg = awsCfg
                     , cfgS3Cfg = s3cfg
                     , cfgLoggerLock = loggerLock
                     , cfgStdoutLock = stdoutLock
                     , cfgManager = mgr
                     , cfgBucketName = bucketName
                     , cfgReadFileThrottle = throttle
                     }

    let (fromDateText, toDateText)
          | [] <- otherArgs
          = ("2013-03-10T10:10", "2013-03-10T10:14")
          | datePattern : _ <- otherArgs
          = case T.splitOn "/" (T.pack datePattern) of
              [start] -> (start, "")
              start:end:_ -> (start, end)
              _ -> error "splitOn returned empty list"

    let Just fromDate = parseDatePattern fromDateText
        Just toDate   = parseDatePattern toDateText
        range = Range fromDate toDate

    all_servers <- Streams.toList =<< lsS3 cfg rootPath
    let !servers = {- take 10 -} all_servers

    queues <- forM servers $ \server -> do
                q <- newTBQueueIO 10
                worker <- async (processServer cfg server range q)
                return (server, q, worker)

    grabAndDistributeFiles cfg queues

type ServerPath = S3Path

type FileNameProducer = (ServerPath, TBQueue (Maybe S3ObjectKey), Async ())

nextAvailableMinutes :: [FileNameProducer]
                     -> IO [(S3ObjectKey, FileNameProducer)]
nextAvailableMinutes [] = return []
nextAvailableMinutes producers = do
  mb_pairs
    <- forM producers $ \ producer@(_,q,worker) -> do
         mb_minute <- atomically (peekTBQueue q)
         case mb_minute of
           Nothing -> cancel worker >> return Nothing
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
           return (Just (producer, lineVar))
  return (catMaybes lineProducers)

grabAndDistributeFiles :: Config -> [FileNameProducer] -> IO ()
grabAndDistributeFiles _ [] = return ()
grabAndDistributeFiles cfg producers0 = do
    queue <- newTBQueueIO 2
    worker <- async (processLineWorker queue)
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
         msg cfg ("OUT: " ++ T.unpack nextMinute)
         t <- getCPUTime
         processAllLines cfg lineProducers B.putStr
         t' <- getCPUTime
         msg cfg ("DONE: " ++ T.unpack nextMinute ++ " "
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

processLines :: Config -> S3Path -> LineBuffer (Maybe Line) -> IO ()
processLines cfg objectPath output = do
  runRequest cfg $ do
    --liftIO $ msg cfg ("OBJECT: " ++ show objectPath)
    t <- liftIO $ getCPUTime
    rsp <- Aws.pureAws (cfgAwsCfg cfg) (cfgS3Cfg cfg)
                       (cfgManager cfg) $!
             S3.getObject (cfgBucketName cfg) objectPath
    let writeLine line =
          liftIO (atomically (pushLine output (Just line)))
    t' <- liftIO $ getCPUTime
    liftIO $ msg cfg ("GOT_OBJECT: " ++ show objectPath ++ " "
           ++ show (fromIntegral (t' - t) / 1000000000.0 :: Double) ++ " ms")
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
   grabNextLine producer@(process, var) !heap = do
     next <- atomically (popLine var)
     case next of
       Nothing -> do
         cancel process
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
                       msg cfg $ "HTTP-Error: " ++ show e ++ "\nRetrying in "
                                  ++ show delayInMS ++ "ms..."
                       threadDelay (delayInMS * 1000)
                       withRetries (n - 1) (delayInMS + delayInMS) act
                      else do
                        msg cfg $ "HTTP-Error: " ++ show e
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
        liftIO $ msg cfg ("REQ: " ++ show path ++
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

matching_ :: Config -> S3Path -> DatePattern -> IO (Response [S3ObjectKey])
matching_ cfg serverPath fromDate =
  go (Just (serverPath <> toMarker fromDate))
 where
   go marker = Sem.with (cfgReadFileThrottle cfg) $ do
     runRequest cfg $ do
       liftIO $ msg cfg ("REQ: " ++ show serverPath ++
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
