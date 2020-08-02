package shardkv

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
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
	CONFIG = 4
	SHARD  = 5
)

type Op struct {
	OpType        int
	Key           string
	Value         string
	OpId          string
	ConfigNum     int
	ReplyReceived []string
	Config        shardmaster.Config
	ShardFinished map[string]string
	ShardData     map[string]string
	Shard         int
}

type AgreeResult struct {
	term      int
	configNum int
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

	cond                 *sync.Cond
	db                   map[string]string
	putAppendFinished    map[string]string
	lastApplied          int
	waitingOption        map[string]chan AgreeResult
	snapshotLastIncluded int
	config               shardmaster.Config
	configHistory        map[int]shardmaster.Config
	shards               map[int]struct{}
	shardsToFetch        map[int]struct{}
	lastMasterLeader     int
	// config num -> shardId -> data
	shardHistory             map[int]map[int]map[string]string
	putAppendFinishedHistory map[int]map[int]map[string]string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	_, hasShard := kv.shards[shard]
	configNum := kv.config.Num
	kv.mu.Unlock()

	if !hasShard {
		DPrintf("No shard!")
		reply.Err = ErrWrongGroup
		return
	}

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:    GET,
		Key:       args.Key,
		ConfigNum: configNum,
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
	shard := key2shard(args.Key)
	kv.mu.Lock()
	_, finished := kv.putAppendFinished[args.OpId]
	_, hasShard := kv.shards[shard]
	configNum := kv.config.Num
	kv.mu.Unlock()

	if finished {
		reply.Err = OK
		return
	}

	if !hasShard {
		reply.Err = ErrWrongGroup
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
		ConfigNum:     configNum,
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

	ch := make(chan AgreeResult, 1)
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

	var result AgreeResult
	select {
	case <-timer.C:
		//log.Printf("[%d] Timeout", kv.me)
		kv.mu.Lock()
		delete(kv.waitingOption, op.OpId)
		kv.mu.Unlock()
		return ErrTimeOut
	case r := <-ch:
		kv.mu.Lock()
		delete(kv.waitingOption, op.OpId)
		kv.mu.Unlock()
		result = r
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	//log.Printf("%d result term is %d", index, resultTerm)
	if result.term == term {
		if result.configNum == kv.config.Num {
			return OK
		} else {
			return ErrWrongGroup
		}
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

		result := AgreeResult{}
		result.term = msg.CommandTerm

		//log.Printf("[%d] apply msg for op %d", kv.me, msg.CommandIndex)
		if msg.Command != nil {
			op := msg.Command.(Op)
			_, finished := kv.putAppendFinished[op.OpId]
			result.configNum = op.ConfigNum

			if !finished && op.ConfigNum == kv.config.Num {
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
				} else if op.OpType == CONFIG {
					if op.Config.Num == kv.config.Num+1 {
						DPrintf("[%d-%d] Apply new config: %d", kv.gid, kv.me, op.Config.Num)
						kv.archiveShard(op.Config)
						kv.configHistory[kv.config.Num] = kv.config
						kv.config = op.Config
					}
				} else if op.OpType == SHARD {
					if _, ok := kv.shardsToFetch[op.Shard]; op.ConfigNum == kv.config.Num && ok {
						kv.mergeShard(op.ShardData, op.ShardFinished)
						kv.shards[op.Shard] = struct{}{}
						delete(kv.shardsToFetch, op.Shard)
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
				case ch <- result:
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
	encoder.Encode(kv.config)
	encoder.Encode(kv.shards)
	encoder.Encode(kv.shardsToFetch)
	encoder.Encode(kv.shardHistory)
	encoder.Encode(kv.configHistory)
	encoder.Encode(kv.putAppendFinishedHistory)
	index := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.Snapshot(w.Bytes(), index)
}

func (kv *ShardKV) decodeSnapshot(msg raft.ApplyMsg) {

	r := bytes.NewBuffer(msg.Command.([]byte))
	decoder := labgob.NewDecoder(r)
	var db map[string]string
	var putAppendFinished map[string]string
	var snapshotLastIncluded int
	var config shardmaster.Config
	var shards map[int]struct{}
	var shardsToFetch map[int]struct{}
	var shardHistory map[int]map[int]map[string]string
	var configHistory map[int]shardmaster.Config
	var putAppendFinishedHistory map[int]map[int]map[string]string

	err1 := decoder.Decode(&db)
	err2 := decoder.Decode(&putAppendFinished)
	err3 := decoder.Decode(&snapshotLastIncluded)
	err4 := decoder.Decode(&config)
	err5 := decoder.Decode(&shards)
	err6 := decoder.Decode(&shardsToFetch)
	err7 := decoder.Decode(&shardHistory)
	err8 := decoder.Decode(&configHistory)
	err9 := decoder.Decode(&putAppendFinishedHistory)

	if err1 != nil || err2 != nil || err3 != nil || err4 != nil ||
		err5 != nil || err6 != nil || err7 != nil || err8 != nil || err9 != nil {
		log.Fatalf("Failed to decode snapshot")
	}

	kv.db = db
	kv.putAppendFinished = putAppendFinished
	kv.snapshotLastIncluded = snapshotLastIncluded
	kv.config = config
	kv.shards = shards
	kv.shardsToFetch = shardsToFetch
	kv.shardHistory = shardHistory
	kv.configHistory = configHistory
	kv.putAppendFinishedHistory = putAppendFinishedHistory
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
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}
		kv.mu.Lock()
		if len(kv.shardsToFetch) > 0 {
			kv.fetchShards()
			kv.mu.Unlock()
		} else {
			latestConfig := kv.config
			kv.mu.Unlock()

			res := kv.queryAndUpdateConfig(latestConfig.Num+1, kv.lastMasterLeader)
			if res {
				continue
			}

			finished := false
			for !finished {
				for i := range kv.masters {
					res = kv.queryAndUpdateConfig(latestConfig.Num+1, i)
					if res {
						finished = true
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) queryAndUpdateConfig(num int, server int) bool {
	var args shardmaster.QueryArgs
	var reply shardmaster.QueryReply
	args.Num = num
	ok := kv.masters[server].Call("ShardMaster.Query", &args, &reply)

	if ok {
		if reply.Err == OK {
			kv.mu.Lock()
			if kv.config.Num+1 == reply.Config.Num {
				if len(kv.shardsToFetch) == 0 {
					kv.startAgreeOnConfig(reply.Config)
				}
			}
			kv.mu.Unlock()
			kv.lastMasterLeader = server
			return true
		}
	}

	return false
}

func (kv *ShardKV) startAgreeOnConfig(config shardmaster.Config) {
	configNum := kv.config.Num
	op := Op{
		OpType:    CONFIG,
		Config:    config,
		ConfigNum: configNum,
	}
	kv.rf.Start(op)
}

func (kv *ShardKV) fetchShards() {
	for shard := range kv.shardsToFetch {
		from := kv.configHistory[kv.config.Num-1].Shards[shard]
		configNum := kv.config.Num
		go func(shard int, from int) {
			DPrintf("[%d-%d] fetching shard: %d", kv.me, kv.gid, shard)
			args := FetchShardsArgs{
				ConfigNum: configNum,
				Shard:     shard,
			}

			for _, server := range kv.configHistory[kv.config.Num-1].Groups[from] {
				reply := FetchShardsReply{}
				DPrintf("[%d-%d] waiting fetching shard: %d", kv.me, kv.gid, shard)
				ok := kv.make_end(server).Call("ShardKV.FetchShard", &args, &reply)

				if ok {
					DPrintf("[%d-%d] fetching shard reply: %s", kv.me, kv.gid, reply.Err)
					if reply.Err == OK {
						op := Op{
							OpType:        SHARD,
							ShardData:     reply.Data,
							Shard:         shard,
							ConfigNum:     configNum,
							ShardFinished: reply.PutAppendFinished,
						}
						kv.rf.Start(op)
						return
					}
				} else {
					DPrintf("[%d-%d] fetch shard failed", kv.me, kv.gid)
					//log.Println("[%d-%d] ", )
				}
			}
		}(shard, from)
	}
}

func (kv *ShardKV) archiveShard(config shardmaster.Config) {
	// shards need to be fetched
	nextShardsToFetch := make(map[int]struct{})
	// shards to server in next config
	nextShards := make(map[int]struct{})

	if config.Num == 1 {
		for shard, group := range config.Shards {
			if group == kv.gid {
				nextShards[shard] = struct{}{}
			}
		}
	} else {
		// shard to be archived
		move := make(map[int]struct{})
		if _, ok := config.Groups[kv.gid]; ok {
			for shard, group := range config.Shards {
				if group == kv.gid {
					nextShardsToFetch[shard] = struct{}{}
				}
			}

			for shardId := range kv.shards {
				// see if this shard is still on this group
				if config.Shards[shardId] == kv.gid {
					nextShards[shardId] = struct{}{}
					delete(nextShardsToFetch, shardId)
				} else {
					move[shardId] = struct{}{}
				}
			}
		} else {
			move = kv.shards
		}

		kv.shardHistory[config.Num] = make(map[int]map[string]string)

		// key: shard, value: data
		for k, v := range kv.db {
			shardOfKey := key2shard(k)
			if _, ok := move[shardOfKey]; ok {
				if _, ok := kv.shardHistory[config.Num][shardOfKey]; !ok {
					kv.shardHistory[config.Num][shardOfKey] = make(map[string]string)
				}
				kv.shardHistory[config.Num][shardOfKey][k] = v
				delete(kv.db, k)
			}
		}

		kv.putAppendFinishedHistory[config.Num] = make(map[int]map[string]string)
		for opId, key := range kv.putAppendFinished {
			shardOfKey := key2shard(key)
			if _, ok := move[shardOfKey]; ok {
				if _, ok := kv.putAppendFinishedHistory[config.Num][shardOfKey]; !ok {
					kv.putAppendFinishedHistory[config.Num][shardOfKey] = make(map[string]string)
				}
				kv.putAppendFinishedHistory[config.Num][shardOfKey][opId] = key
				delete(kv.putAppendFinished, opId)
			}
		}
	}

	kv.shards = nextShards
	kv.shardsToFetch = nextShardsToFetch
}

func (kv *ShardKV) mergeShard(data map[string]string, putAppendFinished map[string]string) {
	for k, v := range data {
		kv.db[k] = v
	}

	for opId, key := range putAppendFinished {
		kv.putAppendFinished[opId] = key
	}
}

func (kv *ShardKV) FetchShard(args *FetchShardsArgs, reply *FetchShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if history, ok := kv.shardHistory[args.ConfigNum]; ok {
		reply.Err = OK
		reply.Data = history[args.Shard]
		reply.PutAppendFinished = kv.putAppendFinishedHistory[args.ConfigNum][args.Shard]
	} else {
		reply.Err = ErrConfigNoMatch
	}
	return
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
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
	kv.waitingOption = make(map[string]chan AgreeResult)
	kv.gid = gid
	kv.masters = masters
	kv.make_end = make_end
	kv.configHistory = make(map[int]shardmaster.Config)
	kv.shardHistory = make(map[int]map[int]map[string]string)
	kv.putAppendFinishedHistory = make(map[int]map[int]map[string]string)
	DPrintf("[%d-%d] initial config: %d", kv.me, kv.gid, kv.config.Num)
	go kv.chanListener()
	if maxraftstate > 0 {
		go kv.raftStateSizeMonitor()
	}
	go kv.configChangeMonitor()

	return kv
}
