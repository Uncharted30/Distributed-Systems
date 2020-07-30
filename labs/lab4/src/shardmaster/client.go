package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"strconv"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeader int
	mu sync.Mutex
	cid int64
	replyReceived []string
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
	ck.cid = nrand()
	return ck
}

func (ck *Clerk) getNextOpId() string {
	return strconv.FormatInt(ck.cid, 10) + strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (ck *Clerk) Query(num int) Config {

	var reply QueryReply
	args := &QueryArgs{
		Num:           num,
	}
	ck.mu.Lock()
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	ok := ck.servers[lastLeader].Call("ShardMaster.Query", args, &reply)
	if ok && reply.Err == OK {
		return reply.Config
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.Err == OK {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	var reply JoinReply
	ck.mu.Lock()
	args := &JoinArgs{
		Servers:       servers,
		OpId:          ck.getNextOpId(),
		ReplyReceived: ck.replyReceived,
	}
	ck.replyReceived = make([]string, 0)
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	ok := ck.servers[lastLeader].Call("ShardMaster.Join", args, &reply)
	if ok && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.Err == OK {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	var reply LeaveReply
	ck.mu.Lock()
	args := &LeaveArgs{
		GIDs:          gids,
		OpId:          ck.getNextOpId(),
		ReplyReceived: ck.replyReceived,
	}
	lastLeader := ck.lastLeader
	ck.replyReceived = make([]string, 0)
	ck.mu.Unlock()

	ok := ck.servers[lastLeader].Call("ShardMaster.Leave", args, &reply)
	if ok && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.Err == OK {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	var reply MoveReply
	ck.mu.Lock()
	args := &MoveArgs{
		Shard:         shard,
		GID:           gid,
		OpId:          ck.getNextOpId(),
		ReplyReceived: ck.replyReceived,
	}
	ck.replyReceived = make([]string, 0)
	lastLeader := ck.lastLeader
	ck.mu.Unlock()

	ok := ck.servers[lastLeader].Call("ShardMaster.Move", args, &reply)
	if ok && reply.Err == OK {
		return
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.Err == OK {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
