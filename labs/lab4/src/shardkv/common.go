package shardkv

import "log"
import "../shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// For debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrTimeOut       = "ErrTimeOut"
	ErrConfigNoMatch = "ConfigNoMatch"
	ErrConfigPast    = "ConfigPast"
)

type Err string

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	OpId          string
	ReplyReceived []string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	ShardPutAppendFinished map[string]string
	OpId                   string
	Shard                  int
	Data                   map[string]string
}

type MoveShardReply struct {
	Err Err
}

type FetchShardsArgs struct {
	ConfigNum int
	Shard     int
}

type FetchShardsReply struct {
	Err  Err
	Data map[string]string
}
