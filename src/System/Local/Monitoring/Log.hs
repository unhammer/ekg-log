{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This module lets you periodically flush metrics to a log file
-- that automatically rotates itself.
--
-- > main = do
-- >     store <- newStore
-- >     forkEkgLog defaultLogOptions store
--
-- You probably want to include some of the predefined metrics defined
-- in the ekg-core package, by calling e.g. the 'registerGcStats'
-- function defined in that package.
module System.Local.Monitoring.Log
    (
      -- * The log syncer
      Log
    , logThreadId
    , forkEkgLog
    , LogOptions(..)
    , defaultLogOptions
    ) where

import           Control.Concurrent       (ThreadId, myThreadId, threadDelay,
                                           throwTo)
import           Control.Exception        ()
import           Control.Monad            (forM_, when)
import           Data.Aeson               as A
import qualified Data.ByteString.Char8    as B8
import qualified Data.ByteString.Lazy     as L
import qualified Data.HashMap.Strict      as M
import           Data.Int                 (Int64)
import           Data.Monoid              ((<>))
import qualified Data.Text                as T
import qualified Data.Text.IO             ()
import           Data.Time
import           Data.Time.Clock.POSIX    (getPOSIXTime)
import qualified System.FilePath          as FP
import           System.IO                (stderr)
import           System.Log.FastLogger
import qualified System.Metrics           as Metrics
import           System.PosixCompat.Files (fileSize, getFileStatus)

#if __GLASGOW_HASKELL__ >= 706
import           Control.Concurrent       (forkFinally)
#else
import           Control.Concurrent       (forkIO)
import           Control.Exception        (SomeException, mask, try)
import           Prelude                  hiding (catch)
#endif

-- | A handle that can be used to control the log sync thread.
-- Created by 'forkEkgLog'.
data Log = Log
    { threadId :: {-# UNPACK #-} !ThreadId
    }

-- | The thread ID of the log sync thread. You can stop the sync by
-- killing this thread (i.e. by throwing it an asynchronous
-- exception.)
logThreadId :: Log -> ThreadId
logThreadId = threadId

-- | Options to control whate log file to append to, when to rotate,
-- and how often to flush metrics. The flush interval should be
-- shorter than the flush interval.
data LogOptions = LogOptions
    { -- | Server hostname or IP address
      logfile       :: !FP.FilePath

      -- | Data push interval, in ms.
    , flushInterval :: !Int

      -- | Print debug output to stderr.
    , debug         :: !Bool

      -- | Prefix to add to all metric names.
    , prefix        :: !T.Text

      -- | Suffix to add to all metric names. This is particularly
      -- useful for sending per application stats.
    , suffix        :: !T.Text
    }

-- | Defaults:
--
-- * @logfile@ = @\"./ekg.log\"@
--
-- * @flushInterval@ = @5000@
--
-- * @debug@ = @False@
defaultLogOptions :: LogOptions
defaultLogOptions = LogOptions
    { logfile       = "./ekg.log"
    , flushInterval = 10000
    , debug         = False
    , prefix        = ""
    , suffix        = ""
    }

-- | Create a thread that periodically flushes the metrics in the
-- store to a log file.
forkEkgLog :: LogOptions    -- ^ Options
           -> Metrics.Store -- ^ Metric store
           -> IO Log        -- ^ Log sync handle
forkEkgLog opts store = do
    logset <- newFileLoggerSet defaultBufSize (logfile opts)
    me     <- myThreadId
    tid    <- forkFinally (loop store logset opts) $ \ r -> do
        case r of
            Left e  -> throwTo me e
            Right _ -> return ()
    return $ Log tid

loop :: Metrics.Store   -- ^ Metric store
     -> LoggerSet       -- ^ FastLogger set
     -> LogOptions      -- ^ Options
     -> IO ()
loop store logset opts = do
    start <- time
    sample <- Metrics.sampleAll store

    logstat <- getFileStatus (logfile opts)
    let logsize = fileSize logstat

    rmLoggerSet logset
    logset' <- newFileLoggerSet defaultBufSize (logfile opts)

    flushOnSize logsize sample logset'

    end <- time
    threadDelay (flushInterval opts * 1000 - fromIntegral (end - start))
    loop store logset' opts

  where
    flushOnSize sz diff logset' | sz == 0   = flushSample diff logset' opts
                                | otherwise = return ()

-- | Microseconds since epoch.
time :: IO Int64
time = (round . (* 1000000.0) . toDouble) `fmap` getPOSIXTime
  where toDouble = realToFrac :: Real a => a -> Double

flushSample :: Metrics.Sample -> LoggerSet -> LogOptions -> IO ()
flushSample sample logset opts = do
    forM_ (M.toList sample) $ \(name, val) -> do
        time' <- getCurrentTime
        let newName = dottedPrefix <> name <> dottedSuffix
            newObj  = case val of
                (Metrics.Counter v)      -> object [ "timestamp" .= time', newName .= show v ]
                (Metrics.Gauge   v)      -> object [ "timestamp" .= time', newName .= show v ]
                (Metrics.Label   v)      -> object [ "timestamp" .= time', newName .= show v ]
                (Metrics.Distribution v) -> object [ "timestamp" .= time', newName .= show v ]
        flushMetric newObj
  where
    isDebug = debug opts

    dottedPrefix = if T.null (prefix opts) then "" else prefix opts <> "."
    dottedSuffix = if T.null (suffix opts) then "" else "." <> suffix opts

    flushMetric metricObj = do
        let !msg = L.toStrict $ A.encode metricObj
        when isDebug $ B8.hPutStrLn stderr $ B8.concat [ "DEBUG: ", msg]
        pushLogStr logset . toLogStr $ B8.append "\n" msg

------------------------------------------------------------------------
-- Backwards compatibility shims

#if __GLASGOW_HASKELL__ < 706
forkFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkFinally action and_then =
  mask $ \restore ->
    forkIO $ try (restore action) >>= and_then
#endif
