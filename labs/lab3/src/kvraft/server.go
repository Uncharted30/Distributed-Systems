package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
)

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Op struct {
	OpType int
	Key    string
	Value  string
	OpId string
	ReplyReceived []string
}

// used to describe a set using map
type EmptyStruct struct {}
var void EmptyStruct

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applied map[int]raft.ApplyMsg
	cond *sync.Cond
	db map[string]string
	putAppendFinished map[string]EmptyStruct
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType: GET,
		Key:    args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[%d] Start waiting for GET response of op index %d...", kv.me, index)

	kv.mu.Lock()
	for !kv.killed() {
		kv.cond.Wait()
		msg, ok := kv.applied[index]
		if ok {
			if msg.CommandTerm == term {
				op = msg.Command.(Op)
				v, ok := kv.db[op.Key]
				if !ok {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = v
				}
			} else {
				reply.Err = ErrWrongLeader
			}
			break
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, finished := kv.putAppendFinished[args.OpId]
	kv.mu.Unlock()
	if finished {
		reply.Err = OK
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Key:    args.Key,
		Value:  args.Value,
		ReplyReceived: args.ReplyReceived,
		OpId: args.OpId,
	}

	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	for !kv.killed() {
		DPrintf("[%d] Start waiting for PUT/APPEND response of op index %d...", kv.me, index)
		kv.cond.Wait()
		msg, ok := kv.applied[index]
		if ok {
			if msg.CommandTerm == term {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
			break
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) chanListener() {
	for msg := range kv.applyCh {
		DPrintf("[%d] Get msg from applyCh for op index %d", kv.me, msg.CommandIndex)
		kv.mu.Lock()
		DPrintf("[%d] Get Lock", kv.me)

		if msg.Command != nil {
			op := msg.Command.(Op)
			_, finished := kv.putAppendFinished[op.OpId]

			if !finished {
				if op.OpType == PUT {
					kv.db[op.Key] = op.Value
					kv.putAppendFinished[op.OpId] = void
				} else if op.OpType == APPEND {
					value, ok := kv.db[op.Key]
					if ok {
						kv.db[op.Key] = value + op.Value
					} else {
						kv.db[op.Key] = op.Value
					}
					kv.putAppendFinished[op.OpId] = void
				}

				if op.ReplyReceived != nil {
					go kv.removeReceived(op.ReplyReceived)
				}
			}
		}

		kv.applied[msg.CommandIndex] = msg
		kv.cond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *KVServer) removeReceived(opIds []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, opId := range opIds {
		delete(kv.putAppendFinished, opId)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.cond = sync.NewCond(&kv.mu)
	kv.db = make(map[string]string)
	kv.applied = make(map[int]raft.ApplyMsg)
	kv.putAppendFinished = make(map[string]EmptyStruct)
	go kv.chanListener()

	return kv
}
