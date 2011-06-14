module MyNetLazy (
	module Network.Socket.ByteString.Lazy,
	getContentsWith
) where

import Network.Socket.ByteString.Lazy

import Control.Monad (liftM)
import Data.ByteString.Lazy.Internal (ByteString(..), defaultChunkSize)
import Network.Socket (Socket(..), ShutdownCmd(..), shutdown)
import System.IO.Unsafe (unsafeInterleaveIO)

import qualified Data.ByteString as S
import qualified Network.Socket.ByteString as N


getContentsWith :: Socket             -- ^ Connected socket
                -> (Socket -> IO ())  -- ^ On shutdown
                -> IO ByteString      -- ^ Data received
getContentsWith sock quit = loop where
	loop = unsafeInterleaveIO $ do
		s <- N.recv sock defaultChunkSize
		if S.null s
		then quit sock >> return Empty
		else Chunk s `liftM` loop
