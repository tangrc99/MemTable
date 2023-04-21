package client

import "github.com/tangrc99/MemTable/utils/readline"

// AddRedisCompletions 注册所有的 redis 命令以及帮助
func AddRedisCompletions(completer *readline.Completer) {

	/////////////// key /////////////////
	completer.Register(readline.NewHint("del", "del key [key ...]"))
	completer.Register(readline.NewHint("exists", "exists key [key ...]"))
	completer.Register(readline.NewHint("keys", "keys [pattern]"))
	completer.Register(readline.NewHint("ttl", "ttl key"))
	completer.Register(readline.NewHint("expire", "expire key seconds"))
	completer.Register(readline.NewHint("pexpire", "pexpire key milliseconds"))
	completer.Register(readline.NewHint("rename", "rename key newkey"))
	completer.Register(readline.NewHint("type", "type key"))
	completer.Register(readline.NewHint("randomkey", "randomkey"))

	/////////////// string /////////////////
	completer.Register(readline.NewHint("set", "set key value"))
	completer.Register(readline.NewHint("get", "get key"))
	completer.Register(readline.NewHint("getset", "getset key value"))
	completer.Register(readline.NewHint("strlen", "strlen key"))
	completer.Register(readline.NewHint("getrange", "getrange key start end"))
	completer.Register(readline.NewHint("setrange", "setrange key offset value"))
	completer.Register(readline.NewHint("mget", "mget key [key ...]"))
	completer.Register(readline.NewHint("mset", "mset key value [key value ...]"))
	completer.Register(readline.NewHint("incr", "incr key"))
	completer.Register(readline.NewHint("incrby", "incrby key increment"))
	completer.Register(readline.NewHint("decr", "decr key"))
	completer.Register(readline.NewHint("decrby", "decrby key decrement"))
	completer.Register(readline.NewHint("append", "append key value"))

	/////////////// hash /////////////////
	completer.Register(readline.NewHint("hset", ""))
	completer.Register(readline.NewHint("hget", ""))
	completer.Register(readline.NewHint("hexists", ""))
	completer.Register(readline.NewHint("hdel", ""))
	completer.Register(readline.NewHint("hmset", ""))
	completer.Register(readline.NewHint("hmget", ""))
	completer.Register(readline.NewHint("hgetall", ""))
	completer.Register(readline.NewHint("hkeys", ""))
	completer.Register(readline.NewHint("hvals", ""))
	completer.Register(readline.NewHint("hincrby", ""))
	completer.Register(readline.NewHint("hlen", ""))
	completer.Register(readline.NewHint("hstrlen", ""))
	completer.Register(readline.NewHint("hrandfield", ""))

	/////////////// list /////////////////
	completer.Register(readline.NewHint("llen", "llen key"))
	completer.Register(readline.NewHint("lpush", "lpush key element [element ...]"))
	completer.Register(readline.NewHint("lpop", "lpop key [count]"))
	completer.Register(readline.NewHint("rpush", "rpush key element [element ...]"))
	completer.Register(readline.NewHint("rpop", "rpop key [count]"))
	completer.Register(readline.NewHint("lindex", "lindex key index"))
	completer.Register(readline.NewHint("lpos", "lpos key element"))
	completer.Register(readline.NewHint("lset", "lset key index element"))
	completer.Register(readline.NewHint("lrem", "lrem key count element"))
	completer.Register(readline.NewHint("lrange", "lrange key start stop"))
	completer.Register(readline.NewHint("ltrim", "ltrim key start stop"))
	completer.Register(readline.NewHint("lmove", "lmove source destination LEFT|RIGHT LEFT|RIGHT"))

	/////////////// set /////////////////
	completer.Register(readline.NewHint("sadd", "sadd key member [member ...]"))
	completer.Register(readline.NewHint("scard", "scard key"))
	completer.Register(readline.NewHint("sismember", "sismember key member"))
	completer.Register(readline.NewHint("srem", "srem key member [member ...]"))
	completer.Register(readline.NewHint("smembers", "smembers key"))
	completer.Register(readline.NewHint("spop", "spop key [count]"))
	completer.Register(readline.NewHint("srandmember", "srandmember key [count]"))
	completer.Register(readline.NewHint("smove", "smove source destination member"))
	completer.Register(readline.NewHint("sdiff", "sdiff key [key ...]"))
	completer.Register(readline.NewHint("sdiffstore", "sdiffstore destination key [key ...]"))
	completer.Register(readline.NewHint("sinter", "sinter key [key ...]"))
	completer.Register(readline.NewHint("sinterstore", "sinterstore destination key [key ...]"))
	completer.Register(readline.NewHint("sunion", "sunion key [key ...]"))
	completer.Register(readline.NewHint("sunionstore", "sunionstore destination key [key ...]"))

	/////////////// zset /////////////////
	completer.Register(readline.NewHint("zadd", "zadd key score member [score member ...]"))
	completer.Register(readline.NewHint("zcount", "zcount key min max"))
	completer.Register(readline.NewHint("zcard", "zcard key"))
	completer.Register(readline.NewHint("zrem", "zrem key member [member ...]"))
	completer.Register(readline.NewHint("zincrby", "zincrby key increment member"))
	completer.Register(readline.NewHint("zscore", "zscore key member"))
	completer.Register(readline.NewHint("zrank", "zrank key member"))
	completer.Register(readline.NewHint("zrevrank", "zrevrank key member"))
	completer.Register(readline.NewHint("zremrangebyscore", "zremrangebyscore key min max"))
	completer.Register(readline.NewHint("zremrangebyrank", "zremrangebyrank key start stop"))
	completer.Register(readline.NewHint("zrange", "zrange key min max"))
	completer.Register(readline.NewHint("zrevrange", "zrevrange key start stop"))
	completer.Register(readline.NewHint("zrangebyscore", "zrangebyscore key min max"))
	completer.Register(readline.NewHint("zrevrangebyscore", "zrevrangebyscore key min max"))

	/////////////// bitmap /////////////////
	completer.Register(readline.NewHint("setbit", "setbit key offset value"))
	completer.Register(readline.NewHint("getbit", "getbit key offset"))
	completer.Register(readline.NewHint("bitcount", "bitcount key [start end]"))
	completer.Register(readline.NewHint("bitpos", "bitpos key bit [start] [end]"))

	/////////////// bloom_filter /////////////////
	completer.Register(readline.NewHint("bf.add", ""))
	completer.Register(readline.NewHint("bf.madd", ""))
	completer.Register(readline.NewHint("bf.exists", ""))
	completer.Register(readline.NewHint("bf.mexists", ""))
	completer.Register(readline.NewHint("bf.info", ""))
	completer.Register(readline.NewHint("bf.reserve", ""))

	/////////////// auth /////////////////
	completer.Register(readline.NewHint("auth", "auth [username] password"))
	completer.Register(readline.NewHint("acl", "acl subcommand [argument ...]"))

	/////////////// cluster /////////////////
	completer.Register(readline.NewHint("cluster", "cluster subcommand [argument ...]"))

	/////////////// connection /////////////////
	completer.Register(readline.NewHint("ping", "ping [message]"))
	completer.Register(readline.NewHint("quit", "quit -"))
	completer.Register(readline.NewHint("select", "select index"))
	completer.Register(readline.NewHint("monitor", "monitor -"))

	/////////////// pubsub /////////////////
	completer.Register(readline.NewHint("publish", "publish channel message"))
	completer.Register(readline.NewHint("subscribe", "subscribe channel [channel ...]"))
	completer.Register(readline.NewHint("unsubscribe", "unsubscribe [channel [channel ...]]"))
	completer.Register(readline.NewHint("blpop", "blpop key [key ...] timeout"))
	completer.Register(readline.NewHint("brpop", "brpop key [key ...] timeout"))

	/////////////// connection /////////////////
	completer.Register(readline.NewHint("sync", "sync -"))
	completer.Register(readline.NewHint("psync", "psync replicationid offset"))
	completer.Register(readline.NewHint("replconf", "replconf"))
	completer.Register(readline.NewHint("slaveof", "slaveof host port"))

	/////////////// script /////////////////
	completer.Register(readline.NewHint("eval", "eval script numkeys key [key ...] arg [arg ...]"))
	completer.Register(readline.NewHint("evalsha", "evalsha sha1 numkeys key [key ...] arg [arg ...]"))
	completer.Register(readline.NewHint("script", "script subcommand [argument ...]"))

	/////////////// server /////////////////
	completer.Register(readline.NewHint("shutdown", "shutdown [NOSAVE|SAVE]"))
	completer.Register(readline.NewHint("flushdb", "flushdb [ASYNC|SYNC]"))
	completer.Register(readline.NewHint("flushall", "flushall [ASYNC|SYNC]"))
	completer.Register(readline.NewHint("dbsize", "dbsize -"))
	completer.Register(readline.NewHint("save", "save -"))
	completer.Register(readline.NewHint("bgsave", "bgsave -"))
	completer.Register(readline.NewHint("slowlog", "slowlog subcommand [argument]"))
	completer.Register(readline.NewHint("info", "info [section]"))

	/////////////// transaction /////////////////
	completer.Register(readline.NewHint("multi", "multi -"))
	completer.Register(readline.NewHint("exec", "exec -"))
	completer.Register(readline.NewHint("discard", "discard -"))
	completer.Register(readline.NewHint("watch", "watch key [key ...]"))

}
