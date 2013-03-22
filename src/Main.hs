{-# LANGUAGE OverloadedStrings, BangPatterns, PatternGuards #-}

module Main where

import qualified Aws
import qualified Aws.S3 as S3

import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Concurrent.ParallelIO.Local ( withPool, parallel )
import           Control.Monad ( when )
import           Control.Monad.IO.Class
import qualified Data.ByteString.Char8 as B
import           Data.Conduit ( runResourceT )
import           Data.Maybe ( fromMaybe )
import           Data.Monoid
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified Data.Text as T
import           Network.HTTP.Conduit ( withManager, Manager )
import           System.Environment
import           System.Exit ( exitFailure )
import           System.IO.Streams ( InputStream, Generator, yield )
import qualified System.IO.Streams as Streams

import           DateMatch

data Config = Config 
  { cfgAwsCfg     :: !Aws.Configuration
  , cfgS3Cfg      :: !(S3.S3Configuration Aws.NormalQuery)
  , cfgLoggerLock :: !(MVar ())
  , cfgManager    :: !Manager
  , cfgBucketName :: !T.Text
  }

data Range = Range !DatePattern !DatePattern

msg :: Config -> String -> IO ()
msg Config{ cfgLoggerLock = lock } text =
  withMVar lock (\_ -> putStrLn text)

defaultMaxKeys :: Maybe Int
defaultMaxKeys = Just 130 -- Nothing

main :: IO ()
main = do
  access_key <- fromMaybe "" <$> lookupEnv "AWS_ACCESS_KEY"
  secret_key <- fromMaybe "" <$> lookupEnv "AWS_SECRET_KEY"

  bucketName : rootPath : {- testServer : -} _ <- getArgs

  when (any null [access_key, secret_key]) $ do
    putStrLn $ "ERROR: Access key or secret key missing\n" ++
               "Ensure that environment variables AWS_ACCESS_KEY and " ++
               "AWS_SECRET_KEY\nare defined"
    exitFailure

  let awsCfg = Aws.Configuration
                  { Aws.timeInfo = Aws.Timestamp
                  , Aws.credentials =
                      Aws.Credentials (B.pack access_key)
                                      (B.pack secret_key)
                  , Aws.logger = Aws.defaultLog Aws.Warning }

  let s3cfg = Aws.defServiceConfig :: S3.S3Configuration Aws.NormalQuery

  loggerLock <- newMVar ()

  withManager $ \mgr -> liftIO $ do

    let cfg = Config { cfgAwsCfg = awsCfg
                     , cfgS3Cfg = s3cfg
                     , cfgLoggerLock = loggerLock
                     , cfgManager = mgr
                     , cfgBucketName = T.pack bucketName
                     }

    let Just fromDate = parseDatePattern "2013-03-10T10:10"
        Just toDate   = parseDatePattern "2013-03-10T10:14"
        range = Range fromDate toDate

    all_servers <- Streams.toList =<< lsS3 cfg (T.pack rootPath)
    let !servers = {- take 10 -} all_servers

    objects <- withPool 20 $ \pool -> parallel pool $
                 [ Streams.toList =<< lsObjects cfg serverPath range
                 | serverPath <- servers
                 ]

    mapM_ print objects
    print (length (filter (not . null) objects))

type S3Path = T.Text
type S3ObjectKey = T.Text

lsS3 :: Config -> T.Text -> IO (InputStream T.Text)
lsS3 cfg path = Streams.fromGenerator (chunkedGen (lsS3_ cfg path))

-- | Return an stream of minute file names matching the given date
-- range.  Files are returned in ASCII-betical order.
--
-- The resulting stream is not thread-safe.
lsObjects :: Config
          -> S3Path -- ^ Absolute path of server's log directory
          -> Range  -- ^ Range requested
          -> IO (InputStream S3ObjectKey)
lsObjects cfg serverPath range@(Range _ toRange) = do
  keys <- Streams.fromGenerator (chunkedGen (matching_ cfg serverPath range))
  let !srvLen = T.length serverPath
  let inRange path = isBefore toRange (Date (T.drop srvLen path))
  takeWhileStream inRange keys

--- Internals ----------------------------------------------------------

data Response a
  = Done
  | Full !a
  | More !a (IO (Response a))

takeWhileStream :: (a -> Bool) -> InputStream a -> IO (InputStream a)
takeWhileStream predicate input = Streams.fromGenerator go
 where
   go = do mb_a <- liftIO (Streams.read input)
           case mb_a of
             Nothing -> return ()
             Just a -> if predicate a then yield a >> go else return ()

lsS3_ :: Config -> S3Path -> IO (Response [S3Path])
lsS3_ cfg0@(Config cfg s3cfg _ mgr bucket) path = go Nothing
 where
   go marker = do
      runResourceT $ do
        liftIO $ msg cfg0 ("REQ: " ++ show path ++
                           maybe "" ((" FROM: "++) . show) marker)
        rsp <- Aws.pureAws cfg s3cfg mgr $!
                 S3.GetBucket{ S3.gbBucket = bucket
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

matching_ :: Config -> S3Path -> Range -> IO (Response [S3ObjectKey])
matching_ cfg@(Config awsCfg s3Cfg _ mgr bucket) serverPath (Range fromDate _toDate) =
  go (Just (serverPath <> toMarker fromDate))
 where
   go marker = do
     runResourceT $ do
       liftIO $ msg cfg ("REQ: " ++ show serverPath ++
                          maybe "" ((" FROM: "++) . show) marker)
       rsp <- Aws.pureAws awsCfg s3Cfg mgr $!
                 S3.GetBucket{ S3.gbBucket = bucket
                             , S3.gbPrefix = Just serverPath
                             , S3.gbDelimiter = Nothing
                             , S3.gbMaxKeys = defaultMaxKeys
                             , S3.gbMarker = marker
                             }
       case map S3.objectKey (S3.gbrContents rsp) of  -- REVIEW: use Data.Vector?
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
