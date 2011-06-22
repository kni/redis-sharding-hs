{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Prelude hiding (catch, getContents)
import Control.Concurrent
import Control.Monad (mapM_, forM, forM_)
import Control.Exception (catch, throw, SomeException, IOException, AsyncException (ThreadKilled))
import Data.ByteString.Lazy.Char8 (ByteString, pack, unpack, split)
import Data.Maybe (maybe, fromJust)
import Data.Tuple (fst, snd)
import System.Posix.Signals
import System.Environment (getArgs, getProgName)
import System.Console.GetOpt
import System.Exit
import Network.Socket hiding (recv)

import MyForkManager
import MyNetLazy -- На основе Network.Socket.ByteString.Lazy

import RedisSharding



blksize        = 1024 * 16 -- For recv.
max_buf_length = 1024 * 100


pair :: a -> b -> (a, b)
pair a b = (a, b)

options :: [OptDescr (String, String)]
options = [
	Option [] ["host"]  (ReqArg (pair "host")  "IP")    "host",
	Option [] ["port"]  (ReqArg (pair "port")  "port")  "port",
	Option [] ["nodes"] (ReqArg (pair "nodes") "nodes") "nodes (host1:port1,host2:port2)"
	]



main = withSocketsDo $ do
	installHandler sigPIPE Ignore Nothing

	argv <- getArgs

	let get_opt = case getOpt Permute options argv of (opts, _, _) -> flip lookup opts
	 -- get_opt :: String -> Maybe String -- name -> value

	progName <- getProgName

	case get_opt "nodes" of
		Just _  -> return ()
		Nothing -> putStr (
				"Parameter 'nodes' is required.\n\nUsing example:\n" ++
				progName  ++ "                             --nodes=10.1.1.2:6380,10.1.1.3:6380,...\n" ++
				progName  ++ "                 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,...\n" ++
				progName  ++ " --host=10.1.1.1 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,...\n"
			) >> exitWith ExitSuccess

	host <- maybe (return iNADDR_ANY) inet_addr (get_opt "host")
	let port = (maybe 6379 (\a -> fromIntegral $ read a) (get_opt "port"))::PortNumber
	let servers = split ',' $ pack $ fromJust $ get_opt "nodes"

	sock <- socket AF_INET Stream defaultProtocol
	setSocketOption sock ReuseAddr 1 -- ToDo потом убрать
	setSocketOption sock KeepAlive 1
	bindSocket sock (SockAddrInet port host)
	listen sock 20

	let accepter = accept sock >>= \(c_sock, _) -> forkIO (welcome c_sock servers) >> accepter

	accepter


welcome c_sock servers = withForkManagerDo $ \fm -> do
	setSocketOption c_sock KeepAlive 1

	addr2sMV <- newMVar [] -- Список пар "server address" => "server socket"

	catch (forM_ servers (server c_sock addr2sMV))
		(\e -> print (e::SomeException) >> clean_from_client c_sock addr2sMV)

	-- Получили список пар "server address" => "server socket" после заполнения, дальше он изментся не будет.
	addr2s <- readMVar addr2sMV

	quit <- newEmptyMVar
	let fquit = putMVar quit True >> throw ThreadKilled

	cmds <- newChan      -- Канал для команд
	let set_cmd c = writeChan cmds c
	let get_cmd   = readChan  cmds

	let c_send s = sendAll c_sock s

	forkWithQuit fm fquit (_servers_reader c_sock c_send servers addr2s get_cmd fquit)
	forkWithQuit fm fquit (_client_reader  c_sock c_send servers addr2s set_cmd fquit)

	takeMVar quit
	killAllThread fm >>	waitAllThread fm
	clean_from_client c_sock addr2sMV

	where
		clean_from_client c_sock addr2sMV = do
			takeMVar addr2sMV >>= return . map snd >>= mapM_ sClose
			sClose c_sock

		-- Соединение с сервером
		server c_sock addr2sMV addr = do
			s_sock <- socket AF_INET Stream defaultProtocol
			ia     <- inet_addr (unpack host)
			connect s_sock (SockAddrInet port_number ia)
			setSocketOption s_sock KeepAlive 1

			modifyMVar_ addr2sMV (return . (++) [(addr,s_sock)])

			where
				[host, port] = split ':' addr
				port_number = fromIntegral (read (unpack port))::PortNumber


		forkWithQuit fm fquit io = forkWith fm (catch io (\e -> chokeIOException e >> fquit) )
			where
			chokeIOException :: IOException -> IO ()
			chokeIOException e = return ()


		_client_reader c_sock c_send servers addr2s set_cmd fquit =
			client_reader getContents c_send servers s_send set_cmd fquit
			where
				getContents :: IO ByteString
				getContents = getContentsWith c_sock (\_ -> fquit)

				s_send s_addr s = sendAll (fromJust $ lookup s_addr addr2s) s


		_servers_reader c_sock c_send servers addr2s get_cmd fquit = do
			sss <- forM addr2s (\(s_addr, s_sock) -> do
						s <- getContentsWith s_sock (\_ -> fquit)
						return (s_addr, s_sock, s)
					)
			servers_reader c_send sss get_cmd fquit
