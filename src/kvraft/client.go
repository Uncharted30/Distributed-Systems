package kvraft

import (
	"../labrpc"
	"strconv"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int

	// client id
	// in production, this could be replace by some other unique identification
	// such as MAC address of a computer
	cid int64
	replyReceived []string
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.cid = nrand()
	DPrintf("Cid is %d", ck.cid)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	reply := GetReply{}

	DPrintf("Get request, key is %s", key)

	ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply)

	if ok {
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
	}

	for {
		for i, server := range ck.servers {
			reply = GetReply{}
			ok := server.Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == OK {
					DPrintf("Get %s result is %s", args.Key, reply.Value)
					ck.lastLeader = i
					return reply.Value
				} else if reply.Err == ErrNoKey {
					return ""
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	DPrintf("Put append request, key is %s, value is %s", key, value)

	ck.mu.Lock()
	opId := strconv.FormatInt(ck.cid, 10) + strconv.FormatInt(time.Now().UnixNano(), 10)
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		OpId:          opId,
		ReplyReceived: ck.replyReceived,
	}

	ck.replyReceived = make([]string, 0)
	ck.mu.Unlock()
	reply := PutAppendReply{}

	ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply)

	if ok {
		if reply.Err == OK {
			ck.mu.Lock()
			ck.replyReceived = append(ck.replyReceived, args.OpId)
			ck.mu.Unlock()
			return
		}
	}


	for {
		for i, server := range ck.servers {
			reply = PutAppendReply{}
			ok := server.Call("KVServer.PutAppend", &args, &reply)
			if ok {
				DPrintf("result: %s\n", reply.Err)
				if reply.Err == OK {
					ck.lastLeader = i
					ck.mu.Lock()
					ck.replyReceived = append(ck.replyReceived, args.OpId)
					ck.mu.Unlock()
					DPrintf("Put append request succeed, key is %s, value is %s", key, value)
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
