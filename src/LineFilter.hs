{-# LANGUAGE BangPatterns #-}
module LineFilter
  ( csvLines, sortWithWindow
  , csvLineSink, Continue 
  )
where

import           Control.Monad.IO.Class ( liftIO )
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import           Data.Conduit
import           Data.Conduit.Binary
import           Data.Conduit.Lazy
import qualified Data.Conduit.List as Conduit
import           Data.Word
import           Data.Char
import qualified Data.Heap as Heap

import Debug.Trace

-- | Splits the input CSV file into lines.  Every returned line will
-- end with a newline (0x0a).  Handles quoted input.
csvLines :: Conduit B.ByteString IO B.ByteString
csvLines = go [] Unquoted
 where
   go chunks parserState = do
     mb_chunk <- await
     case mb_chunk of
       Nothing -> do
         -- end of stream
         if null chunks then
           return ()
          else
           yield (B.concat (reverse (B.pack [0x0a] : chunks)))
       Just chunk -> do
         parse chunk chunks parserState
 
   parse !chunk chunks parserState = do
     case findEndOfCsvRecord chunk parserState of
       Right idx -> do
         let !line = B.take idx chunk
         if not (null chunks) then
           yield (B.concat (reverse (line : chunks)))
          else
           yield line
         if idx < B.length chunk then
           parse (B.drop idx chunk) [] Unquoted
          else
           -- fully consumed chunk
           return ()
       Left parserState' ->
         go (chunk : chunks) parserState'

data Quoted = Quoted | Unquoted
  deriving (Eq, Show)

isQuoteOrNewline :: Word8 -> Bool
isQuoteOrNewline c = c == 0x22 || c == 0x0a

findEndOfCsvRecord :: B.ByteString -> Quoted -> Either Quoted Int
findEndOfCsvRecord inp0 state = go state inp0 0
 where
   go Unquoted inp !offs =
     case B.findIndex isQuoteOrNewline inp of   
       Nothing -> Left Unquoted
       Just n
         | B.index inp n == 0x0a -> Right $! offs + n + 1
         | otherwise             ->
           go Quoted (B.drop (n + 1) inp) (offs + n + 1)
   go Quoted inp !offs =
     case B.findIndex (== 0x22) inp of
       Nothing -> Left Quoted
       Just n ->
         go Unquoted (B.drop (n + 1) inp) (offs + n + 1)

-- | Partially sort the input stream via a fixed size buffer.
sortWithWindow :: (Monad m, Ord a) => Int -> Conduit a m a
sortWithWindow size | size < 2 = sortWithWindow 2
sortWithWindow size = do
  let !heap0 = Heap.empty
  heap <- fillHeap size heap0
  genOutput heap
 where
   fillHeap !n !heap
     | n > 0 = do mb_x <- await
                  case mb_x of
                    Just x -> fillHeap (n - 1) (Heap.insert x heap)
                    Nothing -> return heap
     | otherwise = return heap

   genOutput heap = do
     case Heap.uncons heap of
       Nothing ->
         return ()
       Just (next, heap') -> do
         mb_x <- await
         let !heap'' | Nothing <- mb_x = heap'
                     | Just x <- mb_x  = Heap.insert x heap'
         yield next
         genOutput heap''

{-
-- Handles CSV records, which are delimited by newlines but allow quoting.
-- Thus we scan for balanced quotes.
csvSplit :: B.ByteString -> (B.ByteString, B.ByteString)
csvSplit b = B.splitAt index b
 where (StepState _ _ index) = B.foldl' step (StepState False 0 0) b

data StepState = StepState !Bool 
                           {-# UNPACK #-} !Int
                           {-# UNPACK #-} !Int

step :: StepState -> Word8 -> StepState
step (StepState quoted index last) 0x22 = -- '"'
  StepState (not quoted) (index+1) last    
step (StepState False index _   ) 0x0a = -- '\n'
  StepState False (index+1) (index+1)
step (StepState quoted index last) _ =
  StepState quoted (index+1) last
-}

type Continue = Bool

csvLineSink :: (B.ByteString -> IO Continue) -> Sink B.ByteString IO ()
csvLineSink f = loop 
 where 
   loop = do
     mb_line <- await
     case mb_line of
       Nothing -> return ()
       Just line -> do
         continue <- liftIO (f line)
         if continue then loop else return ()

test1 :: IO ()  
test1 = do
        -- Producer m BS = forall i. ConduitM i  BS   m ()
        -- Source m o    =           ConduitM () o    m ()
        -- Conduit i m o =           ConduitM i  o    m ()
        -- Sink i m r =              ConduitM i  Void m r
  testcsv (LB.fromChunks [csv1])
  print "----"
  testcsv csv2
  print "----"
  testcsv csv3
  print "----"
  xs <- Conduit.sourceList [1, 17, 8, 5, 9] $= sortWithWindow 3 $$ Conduit.consume
  print (xs == [1, 5, 8, 9, 17])
 where 
   testcsv csv =
     let src = sourceLbs csv in
     src $= csvLines $$ csvLineSink (\line -> print line >> return True)

   str2bs = B.pack . map (fromIntegral . ord)
   csv1 = str2bs $ unlines ["fo\"uu\nmma\"o,bar,baz"
                           ,"fuu,ber,bez"]
   csv2 = LB.fromChunks $ map str2bs
            ["foo,ba", "r,zau\nmo", "oo,nka", ",nla"]
   csv3 = LB.fromChunks $ map str2bs
            ["foo,b\"a", "r,zau\nmo", "oo,nka", ",n\"la"]
