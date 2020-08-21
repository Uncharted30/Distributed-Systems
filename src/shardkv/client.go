package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"../labrpc"
	"strconv"
	"sync"
)
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	shardLastLeader [shardmaster.NShards]string
	mu sync.Mutex
	cid int64
	replyReceived []string
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.cid = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	//reply := GetReply{}

	shard := key2shard(key)
	DPrintf("Get request, key is %s", key)
	//server := ck.make_end(ck.shardLastLeader[shard])
	//ok := server.Call("ShardKV.Get", &args, &reply)
	//
	//if ok {
	//	if reply.Err == OK {
	//		return reply.Value
	//	} else if reply.Err == ErrNoKey {
	//		return ""
	//	}
	//}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.shardLastLeader[shard] = servers[si]
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	DPrintf("Put append request, key is %s, value is %s", key, value)

	ck.mu.Lock()
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		OpId:          ck.getNextOpId(),
		ReplyReceived: ck.replyReceived,
	}

	ck.replyReceived = make([]string, 0)
	ck.mu.Unlock()
	reply := PutAppendReply{}
	shard := key2shard(key)
	DPrintf("Get request, key is %s", key)
	server := ck.make_end(ck.shardLastLeader[shard])

	ok := server.Call("ShardKV.PutAppend", &args, &reply)

	if ok {
		if reply.Err == OK {
			ck.mu.Lock()
			ck.replyReceived = append(ck.replyReceived, args.OpId)
			ck.mu.Unlock()
			return
		}
	}

	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				//log.Println(reply.Err)
				if ok && reply.Err == OK {
					ck.shardLastLeader[shard] = servers[si]
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(ck.config.Num + 1)
	}
}

func (ck *Clerk) getNextOpId() string {
	return strconv.FormatInt(ck.cid, 10) + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
