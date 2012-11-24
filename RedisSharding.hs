{-# LANGUAGE OverloadedStrings #-}

module RedisSharding (
	client_reader, servers_reader
) where


import Control.Monad (forM_)
import Data.Int (Int64)
import Data.Digest.CRC32 (crc32)
import Data.Maybe (fromJust)
import System.IO (stderr)


import qualified Data.List as L
import qualified Data.ByteString.Char8 as BS
import           Data.ByteString.Lazy.Char8 (ByteString)
import qualified Data.ByteString.Lazy.Char8 as BSL

import RedisParser;


warn = BS.hPutStrLn stderr . BS.concat . BSL.toChunks

showInt :: Int64 -> ByteString
showInt a = BSL.pack $ show a


key2server key servers = servers !! i
	where
		i = fromIntegral $ (toInteger $ crc32 $ key_tag key) `rem` (toInteger $ length servers)

		key_tag key =
			case BSL.last key == '}' && clams /= [] of
				True  -> BSL.drop (1 + last clams) $ BSL.take (BSL.length key - 1) key
				False -> key
			where
				clams = BSL.findIndices (=='{') key


cmd_type =
	init_cmd_type 1 "PING AUTH SELECT FLUSHDB FLUSHALL DBSIZE KEYS" ++
	init_cmd_type 2 "EXISTS TYPE EXPIRE PERSIST TTL MOVE SET GET GETSET SETNX SETEX INCR INCRBY DECR DECRBY APPEND SUBSTR RPUSH LPUSH LLEN LRANGE LTRIM LINDEX LSET LREM LPOP RPOP SADD SREM SPOP SCARD SISMEMBER SMEMBERS SRANDMEMBER ZADD ZREM ZINCRBY ZRANK ZREVRANK ZRANGE ZREVRANGE ZRANGEBYSCORE ZCOUNT ZCARD ZSCORE ZREMRANGEBYRANK ZREMRANGEBYSCORE HSET HGET HMGET HMSET HINCRBY HEXISTS HDEL HLEN HKEYS HVALS HGETALL" ++
	init_cmd_type 3 "DEL MGET" ++
	init_cmd_type 4 "MSET MSETNX" ++
	init_cmd_type 5 "BLPOP BRPOP"
	where
		init_cmd_type t s = map (\a -> (a, t)) $ filter (/= "") $ BS.split ' ' s


client_reader getContents c_send servers s_send set_cmd fquit =
	getContents >>= client_loop
	where
		client_loop :: ByteString -> IO ()
		client_loop s = do
			s <- case multi_bulk_parser s of
				Just (s, as) -> do
					let Just ((Just cmd):args) = as
					let c = BS.concat $ BSL.toChunks cmd
					case lookup c cmd_type of
						Just 1 -> do -- �� ��� �������
							set_cmd (c, [])
							let cs = cmd2stream as
							forM_ servers (\s_addr -> s_send s_addr cs)
						Just 2 -> do -- �� ���������� ������
							let (Just key):_ = args
							let s_addr = key2server key servers
							set_cmd (c, [s_addr])
							let cs = cmd2stream as
							s_send s_addr cs
						Just 3 -> do -- �� ��������� ��������. CMD key1 key2 ... keyN
							let arg_and_s_addr = map (\arg -> (arg, key2server (fromJust arg) servers)) args
							let s_addrs = map snd arg_and_s_addr
							let uniq_s_addrs = L.nub s_addrs
							set_cmd (c, s_addrs)
							mapM_ (\s_addr -> do
									let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
									let cs = cmd2stream (Just (concat [[Just cmd],_args]))
									s_send s_addr cs
								) uniq_s_addrs
						Just 4 -> do -- �� ��������� ��������. CMD key1 value1 key2 value2 ... keyN valueN
							let arg_and_s_addr = map (\(k, v) -> ((k, v), key2server (fromJust k) servers)) $ to_pair args
							let s_addrs = map snd arg_and_s_addr
							let uniq_s_addrs = L.nub s_addrs
							set_cmd (c, s_addrs)
							mapM_ (\s_addr -> do
									let _args = concat $ map (\((k,v),_)-> [k,v]) $
										filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
									let cs = cmd2stream (Just (concat [[Just cmd],_args]))
									s_send s_addr cs
								) uniq_s_addrs
							where
								to_pair []      = []
								to_pair (a:b:l) = (a,b):to_pair l
						Just 5 -> do -- �� ��������� ��������. CMD key1 key2 ... keyN timeout (����������� �������)
							let timeout = last args
							let arg_and_s_addr = map (\arg -> (arg, key2server (fromJust arg) servers)) $ init args
							let s_addrs = map snd arg_and_s_addr
							let uniq_s_addrs = L.nub s_addrs
							case length uniq_s_addrs == 1 of
								False -> c_send $ BSL.concat ["-ERR Keys of the '", cmd, "' command should be on one node; use key tags\r\n"]
								True  -> do
									set_cmd (c, s_addrs)
									mapM_ (\s_addr -> do
											let _args = map fst $ filter ( \(arg, _s_addr) -> _s_addr == s_addr ) arg_and_s_addr
											let cs = cmd2stream (Just (concat [[Just cmd],_args,[timeout]]))
											s_send s_addr cs
										) uniq_s_addrs
						Nothing -> do
							c_send $ BSL.concat ["-ERR unsupported command '", cmd, "'\r\n"]
					return s
				Nothing      -> do
					c_send "-ERR unified protocol error\r\n"
					getContents
			client_loop s



servers_reader c_send sss get_cmd fquit = servers_loop sss
	where
	servers_loop sss = server_responses get_cmd sss c_send fquit >>= servers_loop

				
server_responses get_cmd sss c_send fquit = do
	(cmd, ss) <- get_cmd
	(sss, rs) <- read_responses cmd ss sss
	join_responses cmd ss sss rs -- return sss
	where
		read_responses cmd ss sss = _read_loop sss [] []
			where
				_read_loop []                            new_sss rs = return (new_sss, rs)
				_read_loop ((s_addr, s_sock, s):old_sss) new_sss rs =
					case ss == [] || elem s_addr ss of
						True ->
							case server_parser s of
								Just (s, r) ->
									_read_loop old_sss ((s_addr, s_sock, s ):new_sss) ((s_addr,r):rs)
								Nothing     -> warn (BSL.concat ["Parsing error server response (", lcmd, ")"]) >> fquit >>
									_read_loop old_sss ((s_addr, s_sock, ""):new_sss) rs
									where lcmd = BSL.fromChunks [cmd]
						False ->    _read_loop old_sss ((s_addr, s_sock, s ):new_sss) rs

		join_responses cmd ss sss rs = do
			let lcmd = BSL.fromChunks [cmd]
			let ((_,fr):_) = rs
			case fr of
				RInt fr -> do
					-- �������� ����� ����������.
					let sm = sum $ map (\(RInt r) -> r) (map snd rs)
					c_send (BSL.concat [":", showInt sm, "\r\n"])
					return sss

				RInline fr -> do
					case any (== fr) $ map ( \(RInline r) -> r) (map snd rs) of
						True  -> c_send (BSL.concat [fr, "\r\n"])                 -- ������ ���������.
						False -> c_send "-ERR nodes return different results\r\n" -- ������ ����������.
					return sss

				RBulk fmr -> do
					-- ������� ��� ��� ������� ������ ���� � ������ �������.
					let (Just ctype) = lookup cmd cmd_type
					case ctype == 2 of
						False -> warn $ BSL.concat ["bulk cmd ", lcmd, " with ", showInt ctype, " != 2"]
						True  -> case length rs == 1 of
							False -> warn "logic error"
							True  -> c_send (arg2stream fmr)
					return sss

				RMultiSize fmrs | length rs == 1 && fmrs == -1 -> c_send "*-1\r\n" >> return sss
				RMultiSize fmrs -> do
							c_send (BSL.concat ["*", showInt sm, "\r\n"])
							case sm > 0 of
								False -> return sss
								True  -> case length ss of
									0         -> read_loop sss $ spiral rs -- �� ���� ��� ���
									1         -> read_loop sss $ spiral rs -- � ����� ���� ���
									otherwise -> read_loop sss ss          -- � ������� ���������� ��� �� ������

							where
								sm = sum $ map (\(RMultiSize r) -> r) (map snd rs)

								-- �������, �� ������ � ������� � ��� �� ����� (������). �� ������ ���������.
								-- print $ take 5 $ spiral [ ("a", 3), ("b", 4), ("c", 2), ("d", 0) ]
								spiral a = go a []
									where
										go [] []  = []
										go [] new = go new []
										go ((k,RMultiSize v):t) new
											| v == 0    =     go t new
											| otherwise = k : go t ((k, RMultiSize(v-1)):new)

								read_loop sss []     = return sss
								read_loop sss (h:t)  = do
									new_sss <- mapM read_one sss
									read_loop new_sss t
									where
										read_one (s_addr, s_sock, s)
											| s_addr == h = case server_parser_multi s of
												Just (s, RBulk r) ->
													c_send (arg2stream r) >>
													return (s_addr, s_sock, s)
												Nothing ->
													warn (BSL.concat ["Parsing error server response (", lcmd, ")"]) >> fquit >>
													return (s_addr, s_sock, s)
											| otherwise   = return (s_addr, s_sock, s)
