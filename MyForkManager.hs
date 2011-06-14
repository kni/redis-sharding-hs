module MyForkManager where

import Control.Concurrent
import Control.Exception (mask, bracket, finally)

newtype ForkManager = FM ( MVar [ ( ThreadId, MVar () ) ] )


withForkManagerDo :: (ForkManager -> IO ()) -> IO ()
withForkManagerDo io =
	bracket
		(newMVar [] >>= return . FM)
		(waitForChildren)
		io
	where
		-- Ожидание завержения всех потомкив
		waitForChildren :: ForkManager -> IO ()
		waitForChildren (FM fm) = mapM_ (takeMVar . snd) =<< takeMVar fm


waitAllThread :: ForkManager -> IO ()
waitAllThread (FM fm) = mapM_ (readMVar . snd) =<< readMVar fm

 
forkWith :: ForkManager -> IO () -> IO ThreadId
forkWith (FM fm) io = mask $ \restore -> do
 	mvar   <- newEmptyMVar
 	thr_id <- forkIO $ finally (restore io) $ putMVar mvar ()
 	childs <- takeMVar fm
 	putMVar fm $ (thr_id,mvar):childs
 	return thr_id

 
killAllThread :: ForkManager -> IO ()
killAllThread (FM fm) = mapM_ (killThread . fst) =<< readMVar fm 
