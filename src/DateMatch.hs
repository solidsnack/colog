{-# LANGUAGE GeneralizedNewtypeDeriving, OverloadedStrings #-}
module DateMatch
  ( anyDate,
    Date(..),
    DatePattern(..),
    parseDatePattern, toMarker,
    isAfter, isBefore,
    dayPattern, dayHourPattern, dayHourMinutePattern
  )
where

import qualified Data.Text as T

isIsoDate :: T.Text -> Bool
isIsoDate _ = True  -- FIXME: implement this

newtype Date = Date T.Text

newtype DatePattern = DatePattern T.Text

instance Show DatePattern where show (DatePattern p) = T.unpack p

anyDate :: DatePattern
anyDate = DatePattern ""

parseDatePattern :: T.Text -> Maybe DatePattern
parseDatePattern input =
  -- TODO: input validation.
  --  "2013-03-03T15:49"
  let full = T.take 16 input in
  Just (DatePattern $! T.map slashify full)
 where
   slashify 'T' = '/'
   slashify ':' = '/'
   slashify x   = x

-- | Checks if the date is matches the given date range or occurs after
-- it.
--
-- > isAfter "2013-03" "2013-03-12" == True
-- > isAfter "2013-03" "2013-04-01" == True
-- > isAfter "2013-03" "2013-02-28" == False
isAfter :: DatePattern -> Date -> Bool
isAfter (DatePattern pat) (Date date) =
  pat `T.isPrefixOf` date || pat <= date

-- | Checks if the date is matches the given date range or occurs
-- before it.
--
-- > isBefore "2013-03" "2013-03-12" == True
-- > isBefore "2013-03" "2013-04-01" == True
-- > isBefore "2013-03" "2013-02-28" == False
isBefore :: DatePattern -> Date -> Bool
isBefore (DatePattern pat) (Date date) =
  pat `T.isPrefixOf` date || pat >= date

dayPattern :: DatePattern -> DatePattern
dayPattern (DatePattern pat) = DatePattern (T.take 10 pat)

dayHourPattern :: DatePattern -> DatePattern
dayHourPattern (DatePattern pat) = DatePattern (T.take 13 pat)

dayHourMinutePattern :: DatePattern -> DatePattern
dayHourMinutePattern (DatePattern pat) = DatePattern (T.take 16 pat)

toMarker :: DatePattern -> T.Text
toMarker (DatePattern pat) = pat

test1 :: IO ()
test1 = do
   let passing = and cases
   if not passing then
     mapM_ print cases
    else putStrLn "Pass"
 where
   Just start = parseDatePattern "2013-01"
   Just end   = parseDatePattern "2013-03-03T15:49"
   cases = [ isAfter start (Date "2013-01-02")
           , not (isBefore start (Date "2013-02-02/12/34Z"))
           , isAfter start (Date "2013-02-02/12/34Z")
           , isBefore end (Date "2013-02-02/12/34Z")
           , isBefore end (Date "2013-01-02/12/34Z")
           , isBefore end (Date "2013-03-03/15/49")
           , not (isBefore end (Date "2013-03-03/15/50"))
           -- , isBefore end (Date ""
           ]
test2 :: IO ()
test2 = do
  print (parseDatePattern "2013")
  print (parseDatePattern "2013-03")
  print (parseDatePattern "2013-03-03")
  print (parseDatePattern "2013-03-03T15")
  print (parseDatePattern "2013-03-03T15:49")
