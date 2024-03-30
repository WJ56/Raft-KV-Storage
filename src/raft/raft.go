package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"math/rand"
	"time"

	"sync"
	"sync/atomic"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

//as each Raft peer becomes aware that successive log entries are
//committed, the peer should send an ApplyMsg to the service (or
//tester) on the same server, via the applyCh passed to Make(). set
//CommandValid to true to indicate that the ApplyMsg contains a newly
//committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState int

const (
	follower serverState = iota
	candidate
	leader
)

type logEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int

	cond      *sync.Cond
	applyCh   chan ApplyMsg
	timeout   time.Duration
	timeNow   time.Time
	voteCount int
	leaderWho int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state serverState
	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []logEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		isleader = true
	}
	return rf.currentTerm, isleader
}

type Persist struct {
	CurrentTerm int
	VotedFor    int
	Log         []logEntry

	LastIncludedIndex int
	LastIncludedTerm  int
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//Debug(dInfo, "S%d persist()", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(Persist{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	})
	if err != nil {
		log.Fatalf("S%d Encode Error: %v", rf.me, e)
		//Debug(dInfo, "S%d persist() Error", rf.me)
		return
	}
	raftState := w.Bytes()

	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	//Debug(dInfo, "S%d readPersist()", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persist Persist

	e1 := d.Decode(&persist)
	if e1 != nil {
		log.Fatalf("S%d Decode Error: %v", rf.me, e1)
	} else {
		rf.currentTerm = persist.CurrentTerm
		rf.votedFor = persist.VotedFor
		rf.log = persist.Log
		rf.lastIncludedIndex = persist.LastIncludedIndex
		rf.lastIncludedTerm = persist.LastIncludedTerm
	}
	for i := range rf.peers {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
	}

	// rf.commitIndex = rf.log[len(rf.log)-1].Index
	// 解码快照数据（如果存在）
	snapshotData := rf.persister.ReadSnapshot()
	if snapshotData != nil && len(snapshotData) > 0 {
		// 使用相应的方法解码快照数据并更新状态机
		rf.snapshot = snapshotData
		// rf.cond.Signal()
		rf.commitIndex = rf.lastIncludedIndex
	}
}

//the service says it has created a snapshot that has
//all info up to and including index. this means the
//service no longer needs the log through (and including)
//that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	// Debug(dSnap, "S%d snap index: %d, lastApplied: %d", rf.me, index, rf.lastApplied)
	if index > rf.lastIncludedIndex && index-rf.lastIncludedIndex < len(rf.log) {
		rf.snapshot = snapshot
		rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
		rf.log = rf.log[index-rf.lastIncludedIndex:]
		//if index == rf.log[len(rf.log)-1].Index {
		//	rf.log = rf.log[:1]
		//} else {
		//	rf.log = append(rf.log[:1], rf.log[index-rf.lastIncludedIndex+1:]...)
		//}
		rf.lastIncludedIndex = index
		rf.log[0].Term = rf.lastIncludedTerm
		rf.log[0].Index = rf.lastIncludedIndex
		rf.persist()
		// Debug(dSnap, "S%d snap LII: %d, logSize: %d", rf.me, index, len(rf.log))
	}
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//Receiver implementation:
	//1. Reply immediately if term < currentTerm
	//2. Create new snapshot file if first chunk (offset is 0)
	//3. Write data into snapshot file at given offset
	//4. Reply and wait for more data chunks if done is false
	//5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	//6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	//7. Discard the entire log
	//8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// 合法领导者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state == candidate || rf.state == leader {
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}
	if rf.leaderWho != args.LeaderId {
		rf.leaderWho = args.LeaderId
	}
	rf.timeNow = time.Now()
	rf.timeout = time.Duration(250+(rand.Int63()%250)) * time.Millisecond

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	rf.snapshot = args.Data
	rf.cond.Signal()

	rf.commitIndex = args.LastIncludedIndex

	if len(rf.log) == 1 || (rf.log[len(rf.log)-1].Index <= args.LastIncludedIndex) ||
		(rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term != args.LastIncludedTerm) {
		rf.log = rf.log[:1]
	} else {
		rf.log = append(rf.log[:1], rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
	}

	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()

	// Debug(dTrace, "S%d applied %d - %d, logSize: %d", rf.me, rf.lastApplied, args.LastIncludedIndex, len(rf.log))
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//fmt.Println(rf.me, "sendAppendEntries")
	// Debug(dSnap, "S%d -> S%d Sending snap LII: %d LIT: %d , term: %d",
	// rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.reset()
			return ok
		}
		if rf.nextIndex[server] <= args.LastIncludedIndex {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
	IsPreVote    bool
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower

	//fmt.Println(rf.me, "Receive", args.CandidateId, "RequestVote")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		//fmt.Println(rf.me, "args.Term < rf.currentTerm", args.CandidateId)
		reply.Term = rf.currentTerm
		return
	}
	updateTimeout := false
	if args.Term > rf.currentTerm {
		//fmt.Println("args.Term > rf.currentTerm : ", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = follower
		updateTimeout = true
		rf.votedFor = -1
		rf.persist()
	}
	// TODO：这里很重要：预选举中，可以在一个任期中同时投多票。正式选举中，才只能投一票。
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId || args.IsPreVote {
		lastLogIndex := rf.log[len(rf.log)-1].Index
		lastLogTerm := rf.log[len(rf.log)-1].Term
		// 判断候选者的日志是否至少和自己的一样新
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// 可以投票
			//fmt.Println(rf.me, "可以投票", args.CandidateId)
			// Debug(dVote, "S%d vote S%d, isPre?:%d ", rf.me, args.CandidateId, args.IsPreVote)
			if args.IsPreVote == false {
				// Not Pre_Vote
				rf.votedFor = args.CandidateId
				rf.persist()
				updateTimeout = true
			}
			reply.VoteGranted = true
			reply.Term = args.Term
			// Debug(dVote, "S%d vote S%d's term %d Lterm %d Lindex %d", rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex)
		}
	}
	if updateTimeout == true {
		rf.timeNow = time.Now()
		rf.timeout = time.Duration(250+(rand.Int63()%250)) * time.Millisecond
	}
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println(rf.me, "Send", server, "sendRequestVote")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			//fmt.Println("reply.Term > rf.currentTerm")
			rf.currentTerm = reply.Term
			rf.reset()
			return ok
		}
		if reply.VoteGranted == true && reply.Term == rf.currentTerm {
			if (rf.state == follower && args.IsPreVote == true) || (rf.state == candidate && args.IsPreVote == false) {
				// Debug(dVote, "S%d got S%d's vote， isPre?:%d ", rf.me, server, args.IsPreVote)
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					if args.IsPreVote {
						// Pre_Vote
						// Debug(dVote, "S%d receive most votes, Insert Formal_Vote", rf.me)
						rf.state = candidate
						rf.vote(false)
					} else {
						// Todo : 选举为leader后，需要做的事
						// Debug(dVote, "S%d receive most votes, become leader", rf.me)
						rf.state = leader
						for i := range rf.peers {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
							rf.matchIndex[i] = 0
						}
						go rf.append()
					}
				}
			}
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // term in the conflicting entry (if any)
	ConflictIndex int  // index of first entry with that term (if any)
}

// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Receiver implementation:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Debug(dTrace, "S%d receive S%d's AppendEntries", rf.me, args.LeaderId)
	reply.Success = false
	if args.Term < rf.currentTerm {
		// 过期的，即不合法领导者
		reply.Term = rf.currentTerm
		return
	}
	// 合法领导者
	if args.Term > rf.currentTerm || (rf.state == candidate || rf.state == leader) {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.votedFor = -1
		rf.persist()
	}
	if rf.leaderWho != args.LeaderId {
		rf.leaderWho = args.LeaderId
	}
	rf.timeNow = time.Now()
	rf.timeout = time.Duration(250+(rand.Int63()%250)) * time.Millisecond

	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
		// 不存在
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
		// Debug(dTrace, "S%d respond S%d's ConflictIndex %d", rf.me, args.LeaderId, reply.ConflictIndex)
		return
	}

	//if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
	//	// 不存在
	//	reply.ConflictTerm = -1
	//	reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
	//	return
	//}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 不存在
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex
		// Debug(dTrace, "S%d respond S%d's ConflictIndex %d", rf.me, args.LeaderId, reply.ConflictIndex)
		return
	}

	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		// 存在但不匹配
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		reply.ConflictIndex = args.PrevLogIndex
		for ; reply.ConflictIndex-rf.lastIncludedIndex > 0; reply.ConflictIndex-- {
			if rf.log[reply.ConflictIndex-rf.lastIncludedIndex].Term != reply.ConflictTerm {
				reply.ConflictIndex++
				break
			}
		}
		// Debug(dTrace, "S%d respond S%d's ConflictIndex %d", rf.me, args.LeaderId, reply.ConflictIndex)
		return
	}
	reply.Success = true
	// TODO: 开始遍历检查
	rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1], args.Entries...)
	rf.persist()
	// Debug(dTrace, "S%d append S%d's Entries, log %v", rf.me, args.LeaderId, rf.log)
	//
	//if args.PrevLogIndex+len(args.Entries) < rf.log[len(rf.log)-1].Index {
	//	rf.log = rf.log[:args.PrevLogIndex+len(args.Entries)+1-rf.lastIncludedIndex]
	//}
	//i := 0
	//for ; i < len(args.Entries); i++ {
	//	if args.PrevLogIndex+1+i >= rf.log[len(rf.log)-1].Index+1 {
	//		break
	//	}
	//	if rf.log[args.PrevLogIndex+i+1-rf.lastIncludedIndex].Term != args.Entries[i].Term {
	//		rf.log = rf.log[:args.PrevLogIndex+i+1-rf.lastIncludedIndex]
	//		rf.persist()
	//		break
	//	}
	//}
	//for ; i < len(args.Entries); i++ {
	//	rf.log = append(rf.log, args.Entries[i])
	//	rf.persist()
	//	Debug(dLog2, "S%d Saved Log (%d, %d) %v",
	//		rf.me, args.Entries[i].Term, args.Entries[i].Index, args.Entries[i])
	//}
	//rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log[len(rf.log)-1].Index)))
	//rf.cond.Signal()
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.log[len(rf.log)-1].Index {
			// Debug(dTrace, "S%d args.LeaderCommit %d > rf.log[len(rf.log)-1].Index %d", rf.me, args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Signal()
	}
}

func (rf *Raft) applyToStateMachine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.cond.L.Lock()
	for rf.killed() == false {
		//Debug(dInfo, "rf.cond.Wait()1")
		// Debug(dInfo, "S%d rf.cond.Wait()1.5", rf.me)
		// Debug(dInfo, "S%d: rf.lastApplied %d, rf.commitIndex %d, rf.lastIncludedIndex %d",
		// rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex)
		if rf.lastApplied < rf.lastIncludedIndex && rf.snapshot != nil {
			rf.lastApplied = rf.lastIncludedIndex
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log[len(rf.log)-1].Index &&
			rf.lastApplied+1 > rf.lastIncludedIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex:  rf.log[rf.lastApplied-rf.lastIncludedIndex].Index,
				SnapshotValid: false,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.cond.Wait()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println(rf.me, "sendAppendEntries")
	// Debug(dLog, "S%d -> S%d Sending PLI: %d PLT: %d N: %d LC: %d, term: %d, - %v",
	// rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, rf.currentTerm, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.reset()
			return ok
		} else if rf.state != leader {
			return ok
		} else if reply.Success == true {
			// Todo : 发送成功且不是心跳包
			// Debug(dTrace, "S%d receive S%d's respond AppendEntries is true", rf.me, server)
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			//Debug(dCommit, "Leader S%d has matchIndex[%d] is %d", rf.me, server, rf.matchIndex[server])
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			//Debug(dCommit, "Leader S%d has nextIndex[%d] is %d", rf.me, server, rf.nextIndex[server])
			N := rf.log[len(rf.log)-1].Index
			for ; N > rf.commitIndex && rf.log[N-rf.lastIncludedIndex].Term > rf.currentTerm; N-- {
			}
			for ; N > rf.commitIndex && rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm; N-- {
				//fmt.Println(N)
				count := 1
				for i, matchIdx := range rf.matchIndex {
					if i != rf.me && matchIdx >= N {
						count++
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							// Debug(dTrace, "S%d's commitIndex is %d", rf.me, rf.commitIndex)
							// 应用所有未应用的日志条目到状态机
							rf.cond.Signal()
							break
						}
					}
				}
			}
		} else if reply.Success == false {
			// TODO : 对方不认可
			if reply.ConflictTerm == -1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				// 是否含有term
				f := false
				for i := args.PrevLogIndex; i-rf.lastIncludedIndex > 0; i-- {
					if rf.log[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
						rf.nextIndex[server] = i
						f = true
						break
					}
				}
				if f == false {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}

			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				reply := InstallSnapshotReply{}
				go rf.sendInstallSnapshot(server, &args, &reply)
			}
		}
	}
	return ok
}

//the service using Raft (e.g. a k/v server) wants to start
//agreement on the next command to be appended to Raft's log. if this
//server isn't the leader, returns false. otherwise start the
//agreement and return immediately. there is no guarantee that this
//command will ever be committed to the Raft log, since the leader
//may fail or lose an election. even if the Raft instance has been killed,
//this function should return gracefully.
//
//the first return value is the index that the command will appear at
//if it's ever committed. the second return value is the current
//term. the third return value is true if this server believes it is
//the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	//fmt.Println("Start1")

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == false && rf.state == leader {
		//fmt.Println("Start2")
		// Todo : 开启共识算法
		isLeader = true
		term = rf.currentTerm
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, logEntry{Command: command, Term: term, Index: index})
		rf.persist()
		// rf.append()
		go rf.append()
		// Debug(dLog2, "S%d LOG: (%d, %d) [%v]", rf.me, rf.currentTerm, index, command)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) reset() {
	rf.state = follower
	rf.votedFor = -1
	rf.voteCount = 0
	rf.persist()
	rf.timeNow = time.Now()
	rf.timeout = time.Duration(250+(rand.Int63()%250)) * time.Millisecond
}

func (rf *Raft) append() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeNow = time.Now()
	rf.timeout = 100 * time.Millisecond
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				reply := InstallSnapshotReply{}
				go rf.sendInstallSnapshot(i, &args, &reply)
			} else {
				if rf.matchIndex[i] == rf.log[len(rf.log)-1].Index || rf.nextIndex[i] > rf.log[len(rf.log)-1].Index {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.log[len(rf.log)-1].Index,
						PrevLogTerm:  rf.log[len(rf.log)-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				} else {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.lastIncludedIndex].Term,
						Entries:      rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:],
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				}
			}
		}
	}
}

func (rf *Raft) vote(isPreVote bool) {
	rf.timeNow = time.Now()
	rf.timeout = time.Duration(250+(rand.Int63()%250)) * time.Millisecond
	if isPreVote == false {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()
	}
	rf.voteCount = 1
	for i, _ := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
				IsPreVote:    isPreVote,
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		//fmt.Println(rf.me, rf.state, rf.currentTerm)

		switch rf.state {
		case follower:
			if time.Since(rf.timeNow) >= rf.timeout {
				// Pre Vote
				// Debug(dVote, "S%d Pre Vote in Term%d", rf.me, rf.currentTerm)
				rf.vote(true)
			}
		case candidate:
			if time.Since(rf.timeNow) >= rf.timeout {
				// Formal Vote
				// Debug(dVote, "S%d Pre Vote in Term%d", rf.me, rf.currentTerm)
				rf.vote(false)
			}
		case leader:
			if time.Since(rf.timeNow) >= rf.timeout {
				go rf.append()
			}
		default:
			panic("unhandled default case")
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

//the service or tester wants to create a Raft server. the ports
//of all the Raft servers (including this one) are in peers[]. this
//server's port is peers[me]. all the servers' peers[] arrays
//have the same order. persister is a place for this server to
//save its persistent state, and also initially holds the most
//recent saved state, if any. applyCh is a channel on which the
//tester or service expects Raft to send ApplyMsg messages.
//Make() must return quickly, so it should start goroutines
//for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                sync.Mutex{},
		peers:             peers,
		persister:         persister,
		me:                me,
		dead:              0,
		applyCh:           applyCh,
		timeout:           time.Duration(250+(rand.Int63()%250)) * time.Millisecond,
		timeNow:           time.Now(),
		voteCount:         0,
		leaderWho:         -1,
		state:             follower,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]logEntry, 1),
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}

	rf.cond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyToStateMachine()

	return rf
}
