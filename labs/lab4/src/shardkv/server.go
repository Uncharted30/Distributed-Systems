package shardkv

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET        = 1
	PUT        = 2
	APPEND     = 3
	MOVE_SHARD = 4
)

type Op struct {
	OpType        int
	Key           string
	Value         string
	OpId          string
	ReplyReceived []string
	data          map[string]string
	shard         int
}

type NotifyMsg struct {
	err   Err
	value string
}

const WaitReplyTimeOut = time.Millisecond * 500

// used to describe a set using map
type EmptyStruct struct{}

var void EmptyStruct

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	cond                   *sync.Cond
	db                     map[string]string
	putAppendFinished      map[string]string
	lastApplied            int
	waitingOption          map[string]chan int
	snapshotLastIncluded   int
	configQueue            []shardmaster.Config
	configQueueLock        sync.Mutex
	config                 shardmaster.Config
	shards                 map[int]struct{}
	lastMasterLeader       int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *ShardKV) waitResult(op Op) Err {

	ch := make(chan int, 1)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}

	kv.mu.Lock()
	kv.waitingOption[op.OpId] = ch
	kv.mu.Unlock()
	DPrintf("Start waiting for %d\n", index)
	defer DPrintf("%d finished\n", index)

	timer := time.NewTimer(WaitReplyTimeOut)
	defer timer.Stop()

	resultTerm := -1
	select {
	case <-timer.C:
		//log.Printf("[%d] Timeout", kv.me)
		kv.mu.Lock()
		delete(kv.waitingOption, op.OpId)
		kv.mu.Unlock()
		return ErrTimeOut
	case t := <-ch:
		kv.mu.Lock()
		delete(kv.waitingOption, op.OpId)
		kv.mu.Unlock()
		resultTerm = t
	}

	//log.Printf("%d result term is %d", index, resultTerm)
	if resultTerm == term {
		return OK
	} else {
		return ErrWrongLeader
	}
}

func (kv *ShardKV) chanListener() {
	for msg := range kv.applyCh {
		//DPrintf("[%d] Get msg from applyCh for op index %d", kv.me, msg.CommandIndex)
		//log.Printf("[%d] chanListener acquires lock", kv.me)
		kv.mu.Lock()

		if msg.IsSnapshot {
			kv.decodeSnapshot(msg)
			//log.Printf("[%d] chanListener released lock", kv.me)
			kv.mu.Unlock()
			continue
		}

		//log.Printf("[%d] apply msg for op %d", kv.me, msg.CommandIndex)
		if msg.Command != nil {
			op := msg.Command.(Op)
			_, finished := kv.putAppendFinished[op.OpId]

			if !finished {
				if op.OpType == PUT {
					kv.db[op.Key] = op.Value
					kv.putAppendFinished[op.OpId] = op.Key
				} else if op.OpType == APPEND {
					value, ok := kv.db[op.Key]
					if ok {
						kv.db[op.Key] = value + op.Value
					} else {
						kv.db[op.Key] = op.Value
					}
					kv.putAppendFinished[op.OpId] = op.Key
				} else if op.OpType == MOVE_SHARD {
					for k, v := range op.data {
						kv.db[k] = v
					}
				}

				if op.ReplyReceived != nil {
					go kv.removeReceived(op.ReplyReceived)
				}
			}

			ch, waiting := kv.waitingOption[op.OpId]
			if waiting {
				select {
				case <-time.After(10 * time.Millisecond):
				case ch <- msg.CommandTerm:
				}
			}
		}

		kv.lastApplied = msg.CommandIndex
		//DPrintf("[%d] chanListener released lock", kv.me)
		kv.mu.Unlock()
		kv.cond.Broadcast()
	}
}

func (kv *ShardKV) snapshot() {
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

func (kv *ShardKV) decodeSnapshot(msg raft.ApplyMsg) {

	r := bytes.NewBuffer(msg.Command.([]byte))
	decoder := labgob.NewDecoder(r)
	err1 := decoder.Decode(&kv.db)
	err2 := decoder.Decode(&kv.putAppendFinished)
	err3 := decoder.Decode(&kv.snapshotLastIncluded)

	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatalf("Failed to decode snapshot")
	}
}

func (kv *ShardKV) removeReceived(opIds []string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, opId := range opIds {
		delete(kv.putAppendFinished, opId)
	}
}

func (kv *ShardKV) raftStateSizeMonitor() {
	kv.mu.Lock()
	for !kv.killed() {
		kv.cond.Wait()
		if kv.rf.GetRaftStateSize() > kv.maxraftstate {
			go kv.snapshot()
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) configChangeMonitor() {
	for !kv.killed() {
		var args shardmaster.QueryArgs
		var reply shardmaster.QueryReply
		args.Num = -1

		ok := kv.masters[kv.lastMasterLeader].Call("ShardMaster.Query", &args, &reply)

		if ok {
			if reply.Err == OK {
				kv.mu.Lock()
				if kv.config.Num < reply.Config.Num {
					kv.configQueueLock.Lock()
					kv.configQueue = append(kv.configQueue, reply.Config)
					kv.configQueueLock.Unlock()
				}
				kv.mu.Unlock()
				return
			}
		}

		for {
			for i, master := range kv.masters {
				ok := master.Call("ShardMaster.Query", &args, &reply)
				if ok {
					if reply.Err == OK {
						kv.mu.Lock()
						if kv.config.Num < reply.Config.Num {
							kv.configQueueLock.Lock()
							kv.configQueue = append(kv.configQueue, reply.Config)
							kv.configQueueLock.Unlock()
						}
						kv.mu.Unlock()
						kv.lastMasterLeader = i
						return
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) newConfigApplier() {
	for {
		kv.mu.Lock()
		if len(kv.configQueue) > 0 {
			nextShards := make(map[int]struct{})
			// key: shard, value: gid
			move := make(map[int]struct{})
			nextConfig := kv.configQueue[0]
			if _, ok := nextConfig.Groups[kv.gid]; ok {
				for shardId := range kv.shards {
					// see if this shard is still on this group
					if nextConfig.Shards[shardId] == kv.gid {
						nextShards[shardId] = struct{}{}
					} else {
						move[shardId] = struct{}{}
					}
				}
			}

			// key: shard, value: data
			shards := make(map[int]map[string]string)
			for k, v := range kv.db {
				shardOfKey := key2shard(k)
				if _, ok := move[shardOfKey]; ok {
					shards[shardOfKey][k] = v
					delete(kv.db, k)
				}
			}

			// key: shard, value: putAppend request which is finished but not acknowledged
			shardsPutAppendFinished := make(map[int]map[string]string)
			newPutAppendFinished := make(map[string]string)
			for opId, key := range kv.putAppendFinished {
				shardOfKey := key2shard(key)
				if _, ok := move[shardOfKey]; ok {
					if _, ok := shardsPutAppendFinished[shardOfKey]; !ok {
						shardsPutAppendFinished[shardOfKey] = make(map[string]string)
					}
					shardsPutAppendFinished[shardOfKey][opId] = key
				} else {
					newPutAppendFinished[opId] = key
				}
			}
			kv.putAppendFinished = newPutAppendFinished
			kv.shards = nextShards
			kv.mu.Unlock()

			kv.sendShardToOtherGroups(shards, shardsPutAppendFinished, nextConfig)
			kv.mu.Lock()
			kv.configQueueLock.Lock()
			kv.config = nextConfig
			kv.configQueue = kv.configQueue[1:]
			kv.configQueueLock.Unlock()
			kv.mu.Unlock()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendShardToOtherGroups(shardsToSend map[int]map[string]string,
	shardsPutAppendFinished map[int]map[string]string, config shardmaster.Config) {
	var wg sync.WaitGroup
	for shard, data := range shardsToSend {
		wg.Add(1)
		go kv.sendShard(config.Shards[shard], shard, data, shardsPutAppendFinished[shard], config, &wg)
	}
	wg.Wait()
}

func (kv *ShardKV) sendShard(gid int, shard int, data map[string]string,
	shardPutAppendFinished map[string]string, config shardmaster.Config, wg *sync.WaitGroup) {
	opId := strconv.Itoa(kv.gid) + strconv.FormatInt(time.Now().UnixNano(), 10)
	args := MoveShardArgs{
		Shard:                  shard,
		Data:                   data,
		OpId:                   opId,
		ShardPutAppendFinished: shardPutAppendFinished,
	}

	for {
		for _, serverId := range config.Groups[gid] {
			reply := MoveShardReply{}
			ok := kv.make_end(serverId).Call("ShardKV.MoveShard", &args, &reply)

			if ok {
				if reply.Err == OK {
					wg.Done()
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:        MOVE_SHARD,
		OpId:          args.OpId,
		data:          args.Data,
		shard:         args.Shard,
	}

	kv.mu.Lock()

	res := kv.waitResult(op)

	reply.Err = res
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
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.cond = sync.NewCond(&kv.mu)
	kv.db = make(map[string]string)
	kv.putAppendFinished = make(map[string]string)
	kv.waitingOption = make(map[string]chan int)
	go kv.chanListener()
	if maxraftstate > 0 {
		go kv.raftStateSizeMonitor()
	}
	go kv.configChangeMonitor()

	return kv
}
