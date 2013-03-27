{-# LANGUAGE GeneralizedNewtypeDeriving, OverloadedStrings #-}
module System.Log.Colog.DateMatch
  ( anyDate,
    Date(..),
    DatePrefix(..),
    parseDatePrefix, toMarker,
    isAfter, isBefore,
    dayPattern, dayHourPattern, dayHourMinutePattern
  )
where

import           Control.Applicative
import           Control.Monad ( guard )
import qualified Data.Attoparsec.Text as Atto
import           Data.Attoparsec.Text ( char, decimal, takeWhile1, many1,
                   (<?>) )
import           Data.Char ( isDigit )
import           Data.Function ( on )
import           Data.List ( sortBy, foldl', nubBy )
import           Data.Ord ( comparing )
import qualified Data.Text as T
import           Data.Time.Clock
import           Data.Time.Format ( formatTime )
import           System.Locale ( defaultTimeLocale )

import Debug.Trace

isIsoDate :: T.Text -> Bool
isIsoDate _ = True  -- FIXME: implement this

newtype Date = Date T.Text

newtype DatePrefix = DatePrefix T.Text

instance Show DatePrefix where show (DatePrefix p) = T.unpack p

-- | A pattern that matches any date.
anyDate :: DatePrefix
anyDate = DatePrefix ""

-- | Parse a range of dates, possibly relative to the current time.
--
-- Examples:
-- > "-3m"         =>  (now - 3 min, now)
-- > "-30m/-20m"   =>  (now - 30 min, now - 20 min)
-- > "2013-03-01"  =>  ("2013-03-01*", "2013-03-01*")
-- > "2013-03-01T14:23/+3h"
-- >               =>  ("2013-03-01T14:23*", "2013-03-01T17:23*")
-- > "2013-03-01T14:23/+1m"
-- >               =>  ("2013-03-01T14:23*", "2013-03-01T17:24*")
-- > "2013-02-28/+2d"
-- >               =>  ("2013-02-28*", "2013-03-02*")
parseDateRange :: T.Text -> IO (Maybe (DatePrefix, DatePrefix))
parseDateRange input = return Nothing

data DatePattern
  = UtcPrefix !T.Text
  | RelDate !IsPositive [(Integer, Unit)]
  | Wildcard
  deriving (Eq, Ord, Show)

type IsPositive = Bool

data Unit = Day | Hour | Minute
  deriving (Eq, Ord, Show)

parseDateParser :: Atto.Parser DatePattern
parseDateParser = relativeDate <|> utcPrefix <|> wildcard
 where
   relativeDate = do
     positive <- ((True <$ char '+') <|> (False <$ char '-')) <?> "+/-"
     deltas <- many1 ((,) <$> decimal <*> unit)
     let sortedDeltas = nubBy ((==) `on` snd) $ sortBy (comparing snd) deltas
     if length sortedDeltas /= length deltas then
       fail "Duplicate time offsets with same units"
      else
       return $! RelDate positive sortedDeltas

   utcPrefix = do
     year <- pYear
     components <- optionalInOrder [pMonth, pDay, pHour, pMinute]
     Atto.endOfInput
     return $! UtcPrefix (T.concat (year:components))

   textBetween :: Int -> T.Text -> T.Text -> Atto.Parser T.Text
   textBetween n low high = do
     text <- Atto.take n
     guard (low <= text && text <= high)
     return text

   pYear   = textBetween 4 "0000" "9999"
   pMonth  = textBetween 3 "-01" "-12"
   pDay    = textBetween 3 "-01" "-31"
   pHour   = textBetween 3 "T00" "T23"
   pMinute = textBetween 3 ":00" ":59"

   wildcard = Wildcard <$ Atto.string "..."

   unit = (Day <$ char 'd') <|> (Hour <$ char 'h')
          <|> (Minute <$ (Atto.string "m" <|> Atto.string "min"))

optionalInOrder :: [Atto.Parser a] -> Atto.Parser [a]
optionalInOrder [] = return []
optionalInOrder (p:ps) =
  ((:) <$> p <*> optionalInOrder ps) <|> return []

datePatternToDatePrefix ::
     UTCTime  -- ^ Current Time
  -> Maybe DatePrefix -- ^ Relative dates should be interpreted relative to
                      -- this prefix.
  -> DatePattern
  -> DatePrefix
datePatternToDatePrefix _now _base (UtcPrefix text) = DatePrefix text
datePatternToDatePrefix _now _base Wildcard         = DatePrefix ""
datePatternToDatePrefix now Nothing (RelDate pos units) =
  let diffTime = foldl' accumulateUnits 0 units
      utcTime =
        addUTCTime (fromInteger (if pos then diffTime else (-diffTime))) now
  in
    -- TODO: Set accuracy based on granularity of relative date
    utcTimeToDatePrefix utcTime
-- FIXME: (Just base) 
 where
   accumulateUnits delta (amount, unit) =
     delta + amount * unitToSeconds unit

   unitToSeconds Minute = 60
   unitToSeconds Hour   = 60 * 60
   unitToSeconds Day    = 24 * 60 * 60

utcTimeToDatePrefix :: UTCTime -> DatePrefix
utcTimeToDatePrefix t = DatePrefix . T.pack $
  formatTime defaultTimeLocale "%Y-%m-%dT%H:%M" t

-- | Parse a date pattern from a text.
parseDatePrefix :: T.Text -> Maybe DatePrefix
parseDatePrefix input =
  -- TODO: input validation.
  --  "2013-03-03T15:49"
  let full = T.take 16 input in
  Just (DatePrefix $! T.map slashify full)
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
isAfter :: DatePrefix -> Date -> Bool
isAfter (DatePrefix pat) (Date date) =
  pat `T.isPrefixOf` date || pat <= date

-- | Checks if the date is matches the given date range or occurs
-- before it.
--
-- > isBefore "2013-03" "2013-03-12" == True
-- > isBefore "2013-03" "2013-04-01" == True
-- > isBefore "2013-03" "2013-02-28" == False
isBefore :: DatePrefix -> Date -> Bool
isBefore (DatePrefix pat) (Date date) =
  pat `T.isPrefixOf` date || pat >= date

-- | Returns the part of the pattern that only matches a day.
dayPattern :: DatePrefix -> DatePrefix
dayPattern (DatePrefix pat) = DatePrefix (T.take 10 pat)

-- | Returns the part of the pattern that only matches a day and the
-- hour.
dayHourPattern :: DatePrefix -> DatePrefix
dayHourPattern (DatePrefix pat) = DatePrefix (T.take 13 pat)

-- | Returns the part of the pattern that only matches a day and the
-- hour and the minute.
dayHourMinutePattern :: DatePrefix -> DatePrefix
dayHourMinutePattern (DatePrefix pat) = DatePrefix (T.take 16 pat)

toMarker :: DatePrefix -> T.Text
toMarker (DatePrefix pat) = pat

test1 :: IO ()
test1 = do
   let passing = and cases
   if not passing then
     mapM_ print cases
    else putStrLn "Pass"
 where
   Just start = parseDatePrefix "2013-01"
   Just end   = parseDatePrefix "2013-03-03T15:49"
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
  print (parseDatePrefix "2013")
  print (parseDatePrefix "2013-03")
  print (parseDatePrefix "2013-03-03")
  print (parseDatePrefix "2013-03-03T15")
  print (parseDatePrefix "2013-03-03T15:49")

test3 :: IO ()
test3 = do
  let pp input = print (Atto.parseOnly parseDateParser input)
  pp "+1h"
  pp "-2h"
  pp "+3m1h"
  pp "2013"
  pp "2013-05"
  pp "2013-20"
  pp "abc"
  pp "2013-01-29"
  pp "2013-01-29T14"
  pp "2013-01-29T14:30"

test4 = do
  let parser = do
        optionalInOrder [char 'a', char 'b', char 'c'] <* Atto.endOfInput
  print (Atto.parseOnly parser "ab")
  print (Atto.parseOnly parser "abc")
  print (Atto.parseOnly parser "bc")

test5 = do
  let oldnow = read "2013-03-27 14:02:16.952677 UTC" :: UTCTime
  now <- getCurrentTime
  print (now, oldnow)
  print (datePatternToDatePrefix now Nothing (RelDate False [(5, Minute)]))
  print (datePatternToDatePrefix oldnow Nothing
           (RelDate False [(1, Hour), (5, Minute)]))