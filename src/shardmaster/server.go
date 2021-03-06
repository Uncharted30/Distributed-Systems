package shardmaster

import (
	"../raft"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs         []Config // indexed by config num
	shardAssign     [][]int
	groupMap        []int
	requestFinished map[string]struct{}
	waitingReply    map[string]chan Result
}

type Op struct {
	// Your data here.
	OpId          string
	OpType        OpType
	Servers       map[int][]string
	ReplyReceived []string
	GIDs          []int
	Shard         int
	GID           int
	Num           int
}

type OpType string

const (
	Join  = "Join"
	Move  = "Move"
	Leave = "Leave"
	Query = "Query"
)

const WaitReplyTimeOut = 500 * time.Millisecond

type Result struct {
	ResultTerm  int
	ConfigIndex int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{
		OpId:          args.OpId,
		OpType:        Join,
		Servers:       args.Servers,
		ReplyReceived: args.ReplyReceived,
	}

	err, isLeader, _ := sm.waitResult(op)

	//log.Println(sm.configs[len(sm.configs) - 1])
	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{
		OpId:          args.OpId,
		OpType:        Leave,
		ReplyReceived: args.ReplyReceived,
		GIDs:          args.GIDs,
	}

	err, isLeader, _ := sm.waitResult(op)

	//log.Printf("[%d] Leave result: ", sm.me)
	//log.Print(sm.configs[len(sm.configs) - 1])
	//log.Println()
	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op{
		OpId:   args.OpId,
		OpType: Move,
		GID:    args.GID,
		Shard:  args.Shard,
	}

	err, isLeader, _ := sm.waitResult(op)

	reply.Err = err
	reply.WrongLeader = !isLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	op := Op {
		OpType: Query,
		Num:    args.Num,
	}

	err, isLeader, configIndex := sm.waitResult(op)

	reply.Err = err
	reply.WrongLeader = !isLeader
	DPrintf("[%d] %s", sm.me, err)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	reply.Config = sm.configs[configIndex]
}

func (sm *ShardMaster) waitResult(op Op) (Err, bool, int) {
	index, term, isLeader := sm.rf.Start(op)

	ch := make(chan Result)
	if !isLeader {
		return OK, false, 0
	}

	sm.mu.Lock()
	sm.waitingReply[op.OpId] = ch
	sm.mu.Unlock()

	timer := time.NewTimer(WaitReplyTimeOut)
	defer timer.Stop()

	DPrintf("[%d] start waiting for %s: %d", sm.me, op.OpType, index)

	var res Result
	select {
	case <-timer.C:
		//log.Printf("[%d] Timeout", kv.me)
		sm.mu.Lock()
		delete(sm.waitingReply, op.OpId)
		sm.mu.Unlock()
		return ErrTimeOut, true, 0
	case r := <-ch:
		sm.mu.Lock()
		delete(sm.waitingReply, op.OpId)
		sm.mu.Unlock()
		res = r
	}

	if res.ResultTerm != term {
		return ErrWrongLeader, false, 0
	} else {
		if op.OpType == Query {
			return OK, true, res.ConfigIndex
		}
		return OK, true, 0
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) chanListener() {
	for msg := range sm.applyCh {
		DPrintf("[%d] Get msg from applyCh for op index %d", sm.me, msg.CommandIndex)
		//log.Printf("[%d] chanListener acquires lock", kv.me)
		sm.mu.Lock()

		//log.Printf("[%d] apply msg for op %d", kv.me, msg.CommandIndex)
		if msg.Command != nil {
			op := msg.Command.(Op)
			_, finished := sm.requestFinished[op.OpId]

			res := Result{
				ResultTerm:  msg.CommandTerm,
			}

			if !finished {
				if op.OpId != "" {
					sm.requestFinished[op.OpId] = struct{}{}
				}
				if op.OpType == Join {
					sm.serverJoin(op.Servers)
				} else if op.OpType == Leave {
					sm.serverLeave(op.GIDs)
				} else if op.OpType == Move {
					sm.shardMove(op.Shard, op.GID)
				} else if op.OpType == Query {
					if op.Num == -1 || op.Num >= len(sm.configs) {
						res.ConfigIndex = len(sm.configs) - 1
					} else {
						res.ConfigIndex = op.Num
					}
				}

				if op.ReplyReceived != nil {
					go sm.removeReceived(op.ReplyReceived)
				}
			}

			ch, waiting := sm.waitingReply[op.OpId]
			if waiting {
				select {
				case <-time.After(50 * time.Millisecond):
				case ch <- res:
				}
			}
		}

		//DPrintf("[%d] chanListener released lock", kv.me)
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) serverJoin(servers map[int][]string) {
	lastConfigIndex := len(sm.configs) - 1
	copyGroups := copyMap(sm.configs[lastConfigIndex].Groups)

	newGroupNum := len(copyGroups) + len(servers)
	// avg number of shards that each group would serve
	avg := NShards / newGroupNum
	move := make([]int, 0)

	if len(sm.groupMap) == 0 {
		move = sm.shardAssign[0]
		sm.shardAssign = make([][]int, 0)
	} else {
		for i, assign := range sm.shardAssign {
			groupShardNum := avg
			// groups in the front of the slice may have avg + 1 shards assigned to them
			if avg*newGroupNum+i+1 <= NShards {
				groupShardNum++
			}

			// move out redundant shards
			for len(assign) > groupShardNum {
				move = append(move, assign[len(assign)-1])
				assign = assign[:len(assign)-1]
			}

			sm.shardAssign[i] = assign
		}
	}

	// assign shards to new groups
	for i := 0; i < len(servers); i++ {
		// calculate number of shards this group would serve
		groupShardNum := avg
		if avg*newGroupNum+len(sm.shardAssign) < NShards {
			groupShardNum++
		}
		DPrintf("[%d] groupShardNum: %d, groupNum: %d, move len: %d, new server num: %d", sm.me, groupShardNum, newGroupNum, len(move), len(servers))
		// add shards
		assign := make([]int, 0)
		for len(assign) < groupShardNum {
			assign = append(assign, move[len(move)-1])
			move = move[:len(move)-1]
		}
		sm.shardAssign = append(sm.shardAssign, assign)
	}

	// add new groups to group map
	for gid := range servers {
		sm.groupMap = append(sm.groupMap, gid)
	}

	joinMap(copyGroups, servers)

	sm.addNewConfigFromShardAssign(copyGroups)
	//log.Println(sm.shardAssign)
}

func (sm *ShardMaster) serverLeave(GIDs []int) {
	set := make(map[int]struct{})
	for _, gid := range GIDs {
		set[gid] = struct{}{}
	}

	// to store shardsIds that on the groups that about to leave
	move := make([]int, 0)
	nextGroupMap := make([]int, 0)
	nextShardAssign := make([][]int, 0)
	// number of servers when leave completes

	for i := 0; i < len(sm.shardAssign); i++ {
		gid := sm.groupMap[i]
		if _, ok := set[gid]; ok {
			// shards on this group need to be assigned to other groups
			move = append(move, sm.shardAssign[i]...)
		} else {
			nextGroupMap = append(nextGroupMap, sm.groupMap[i])
			nextShardAssign = append(nextShardAssign, sm.shardAssign[i])
		}
	}

	sm.shardAssign = nextShardAssign
	sm.groupMap = nextGroupMap

	lastConfigIndex := len(sm.configs) - 1
	copyGroups := copyMap(sm.configs[lastConfigIndex].Groups)
	for _, gid := range GIDs {
		delete(copyGroups, gid)
	}

	size := len(sm.groupMap)
	if size == 0 {
		assign := make([]int, 0)
		for i := 0; i < NShards; i++ {
			assign = append(assign, i)
		}
		sm.shardAssign = append(sm.shardAssign, assign)
	} else {
		avg := NShards / size

		for i, assign := range sm.shardAssign {
			// calculate the number of shards this group would serve
			shardNum := avg
			if avg*size+i+1 <= NShards {
				shardNum++
			}

			// add shards to other groups
			for len(assign) < shardNum {
				assign = append(assign, move[len(move)-1])
				move = move[:len(move)-1]
			}

			sm.shardAssign[i] = assign
		}
	}

	sm.addNewConfigFromShardAssign(copyGroups)
}

func (sm *ShardMaster) shardMove(shard int, addGroupId int) {
	lastConfigIndex := len(sm.configs) - 1
	// find the original group that the shard belongs to
	delGroupId := sm.configs[lastConfigIndex].Shards[shard]

	DPrintf("[%d] Move, shard: %d, dest: %d, delGroupId: %d", sm.me, shard, addGroupId, delGroupId)
	//log.Println(sm.groupMap)

	if delGroupId == addGroupId {
		copyGroup := copyMap(sm.configs[lastConfigIndex].Groups)
		sm.addNewConfigFromShardAssign(copyGroup)
		return
	}

	// find the index of delete group and add group
	delGroupIdx := -1
	addGroupIdx := -1
	for i, groupId := range sm.groupMap {
		if groupId == delGroupId {
			delGroupIdx = i
		}
		if groupId == addGroupId {
			addGroupIdx = i
		}
	}

	// take two groups out, than insert them back to correct position
	// because we need these two arrays to be in sorted order
	delGroupShards := sm.shardAssign[delGroupIdx]
	addGroupShards := sm.shardAssign[addGroupIdx]
	if addGroupIdx < delGroupIdx {
		delGroupIdx--
	} else {
		addGroupIdx--
	}
	sm.shardAssign = append(sm.shardAssign[:delGroupIdx], sm.shardAssign[delGroupIdx+1:]...)
	sm.groupMap = append(sm.groupMap[:delGroupIdx], sm.groupMap[delGroupIdx + 1:]...)
	sm.shardAssign = append(sm.shardAssign[:addGroupIdx], sm.shardAssign[addGroupIdx+1:]...)
	sm.groupMap = append(sm.groupMap[:addGroupIdx], sm.groupMap[addGroupIdx + 1:]...)

	// delete shard from original group
	for i, shardId := range delGroupShards {
		if shardId == shard {
			delGroupShards = append(delGroupShards[:i], delGroupShards[i+1:]...)
			break
		}
	}

	// add shards to dest group
	addGroupShards = append(addGroupShards, shard)

	delAppended := false
	addAppended := false
	newShardAssign := make([][]int, 0)
	newGroupMap := make([]int, 0)
	for i := 0; i < len(sm.shardAssign); i++ {
		if len(sm.shardAssign[i]) <= len(delGroupShards) {
			newShardAssign = append(newShardAssign, delGroupShards)
			newGroupMap = append(newGroupMap, delGroupId)
			delAppended = true
		}

		if len(sm.shardAssign[i]) <= len(addGroupShards) {
			newShardAssign = append(newShardAssign, addGroupShards)
			newGroupMap = append(newGroupMap, addGroupId)
			addAppended = true
		}

		newShardAssign = append(newShardAssign, sm.shardAssign[i])
		newGroupMap = append(newGroupMap, sm.groupMap[i])
	}

	if !delAppended && !addAppended {
		if len(delGroupShards) < len(addGroupShards) {
			newShardAssign = append(newShardAssign, addGroupShards)
			newShardAssign = append(newShardAssign, delGroupShards)
			newGroupMap = append(newGroupMap, addGroupId)
			newGroupMap = append(newGroupMap, delGroupId)
		} else {
			newShardAssign = append(newShardAssign, delGroupShards)
			newShardAssign = append(newShardAssign, addGroupShards)
			newGroupMap = append(newGroupMap, delGroupId)
			newGroupMap = append(newGroupMap, addGroupId)
		}
	} else if !delAppended {
		newShardAssign = append(newShardAssign, delGroupShards)
		newGroupMap = append(newGroupMap, delGroupId)
	} else if !addAppended {
		newShardAssign = append(newShardAssign, addGroupShards)
		newGroupMap = append(newGroupMap, addGroupId)
	}

	sm.shardAssign = newShardAssign
	sm.groupMap = newGroupMap

	copyGroup := copyMap(sm.configs[lastConfigIndex].Groups)
	sm.addNewConfigFromShardAssign(copyGroup)
}

func (sm *ShardMaster) removeReceived(opIds []string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, opId := range opIds {
		delete(sm.requestFinished, opId)
	}
}

func (sm *ShardMaster) addNewConfigFromShardAssign(groups map[int][]string) {
	lastConfigIndex := len(sm.configs) - 1

	var shards [NShards]int
	for i, assign := range sm.shardAssign {
		for _, shardId := range assign {
			if len(sm.groupMap) == 0 {
				shards[shardId] = 0
			} else {
				shards[shardId] = sm.groupMap[i]
			}
		}
	}

	newConfig := Config{
		Num:    lastConfigIndex + 1,
		Shards: shards,
		Groups: groups,
	}

	sm.configs = append(sm.configs, newConfig)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.shardAssign = make([][]int, 1)
	sm.waitingReply = make(map[string]chan Result)
	sm.requestFinished = make(map[string]struct{})
	for i := 0; i < NShards; i++ {
		sm.configs[0].Shards[i] = 0
		sm.shardAssign[0] = append(sm.shardAssign[0], i)
	}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.chanListener()

	return sm
}
