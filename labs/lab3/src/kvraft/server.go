package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Op struct {
	OpType        int
	Key           string
	Value         string
	OpId          string
	ReplyReceived []string
}

const WaitReplyTimeOut = time.Millisecond * 500

// used to describe a set using map
type EmptyStruct struct{}

var void EmptyStruct

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cond              *sync.Cond
	db                map[string]string
	putAppendFinished map[string]EmptyStruct
	lastApplied       int
	waitingOption     map[int]chan int
	snapshotLastIncluded int
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

	res := kv.waitResult(op)

	reply.Err = res
	if res == OK {
		kv.mu.Lock()
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
	}
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
		Key:           args.Key,
		Value:         args.Value,
		ReplyReceived: args.ReplyReceived,
		OpId:          args.OpId,
	}

	if args.Op == "Put" {
		op.OpType = PUT
	} else {
		op.OpType = APPEND
	}

	res := kv.waitResult(op)

	reply.Err = res
}

func (kv *KVServer) waitResult(op Op) Err {

	ch := make(chan int, 1)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	kv.waitingOption[index] = ch
	kv.mu.Unlock()
	log.Printf("Start waiting for %d\n", index)
	defer log.Printf("%d finished\n", index)

	timer := time.NewTimer(time.Millisecond * WaitReplyTimeOut)
	defer timer.Stop()

	resultTerm := -1
	select {
		// Looks like this won't fire...
	case <-timer.C:
		log.Println("Timeout")
		kv.mu.Lock()
		delete(kv.waitingOption, index)
		kv.mu.Unlock()
		return ErrTimeOut
	case term := <- ch:
		kv.mu.Lock()
		delete(kv.waitingOption, index)
		kv.mu.Unlock()
		resultTerm = term
	}

	log.Printf("%d result term is %d", index, resultTerm)
	if resultTerm == term {
		return OK
	} else {
		return ErrWrongLeader
	}
}

func (kv *KVServer) chanListener() {
	for msg := range kv.applyCh {
		//DPrintf("[%d] Get msg from applyCh for op index %d", kv.me, msg.CommandIndex)
		kv.mu.Lock()

		if msg.IsSnapshot {
			kv.decodeSnapshot(msg)
			for index, ch := range kv.waitingOption {
				if index <= kv.snapshotLastIncluded {
					ch <- -1
				}
			}
			kv.mu.Unlock()
			continue
		}

		log.Printf("[%d] apply msg for op %d", kv.me, msg.CommandIndex)
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

		ch, waiting := kv.waitingOption[msg.CommandIndex]
		if waiting {
			ch <- msg.CommandTerm
		}
		kv.lastApplied = msg.CommandIndex
		kv.mu.Unlock()
		kv.cond.Broadcast()
	}
}

func (kv *KVServer) snapshot() {
	DPrintf("[%d] snapshotting!!", kv.me)
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	kv.mu.Lock()
	kv.snapshotLastIncluded = kv.lastApplied
	encoder.Encode(kv.db)
	encoder.Encode(kv.putAppendFinished)
	encoder.Encode(kv.snapshotLastIncluded)
	index := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(w.Bytes(), index)
}

func (kv *KVServer) decodeSnapshot(msg raft.ApplyMsg) {

	r := bytes.NewBuffer(msg.Command.([]byte))
	decoder := labgob.NewDecoder(r)
	err1 := decoder.Decode(&kv.db)
	err2 := decoder.Decode(&kv.putAppendFinished)
	err3 := decoder.Decode(&kv.snapshotLastIncluded)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatalf("Failed to decode snapshot")
	}
}

func (kv *KVServer) removeReceived(opIds []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, opId := range opIds {
		delete(kv.putAppendFinished, opId)
	}
}

func (kv *KVServer) raftStateSizeMonitor() {
	kv.mu.Lock()
	for !kv.killed() {
		kv.cond.Wait()
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			go kv.snapshot()
		}
	}
	kv.mu.Unlock()
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
	kv.putAppendFinished = make(map[string]EmptyStruct)
	kv.waitingOption = make(map[int]chan int)
	go kv.chanListener()
	if maxraftstate > 0 {
		go kv.raftStateSizeMonitor()
	}

	return kv
}
