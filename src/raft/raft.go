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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	LEADER    int32 = 0
	FOLLOWER  int32 = 1
	CANDIDATE int32 = 2
	HEARTBEAT       = "HTBT"
	VOTE            = "VOTE"
	LOG             = "LOG"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int64
	votedFor    int
	log         Log

	//volatile
	commitIndex      int
	lastAppliedIndex int
	msgCh            chan LogEntrie
	applyCh          chan ApplyMsg
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	state            int32

	//volatile for leader
	nextIndex  []int64
	matchIndex []int64
}

func (rf *Raft) getLogLen() int {
	return rf.log.logLen()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	var term int = int(atomic.LoadInt64(&rf.currentTerm))
	var isleader bool = (atomic.LoadInt32(&rf.state) == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
//
//	func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//		return ok
//	}

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

func (rf *Raft) leaderCommit() {
	if rf.getLogLen() == 0 {
		return
	}
	var toCommit []LogEntrie
	for _, log := range rf.log.slice(rf.commitIndex+1, -1) {
		var count []int
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				count = append(count, j)
				continue
			}
			if int(atomic.LoadInt64(&rf.matchIndex[j])) >= log.Index {
				count = append(count, j)
			}
		}
		if len(count) <= len(rf.peers)/2 {
			return
		}
		// rf.PortPrintf("for (%d)%v, %v agree", i, log.Command, count)
		toCommit = append(toCommit, log)
		if log.Term == rf.currentTerm {
			rf.commitIndex = log.Index
			for _, log := range toCommit {
				go rf.apply(true, log.Command, log.Index)
				rf.PortPrintf("leader commit: %d,%v", log.Index, log.Command)
			}
			toCommit = toCommit[:0]
		}
	}
}

func (rf *Raft) sendAppendEntries(i int, latestTerm *int64) {
	args := &AppendEntriesArgs{
		Type:         HEARTBEAT,
		Term:         atomic.LoadInt64(&rf.currentTerm),
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.getLogLen() - 1,
	}
	nextIndex := atomic.LoadInt64(&rf.nextIndex[i])
	if rf.getLogLen()-1 >= int(nextIndex) {
		args.Type = LOG
		args.Entries = rf.log.slice(int(nextIndex), -1)
		args.PrevLogIndex = int(nextIndex) - 1
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log.get(args.PrevLogIndex).Term
	}
	reply := &AppendEntriesReply{}
	rf.PortPrintf(" %d type:%s,prevIndex:%d", i, args.Type, args.PrevLogIndex)
	rf.peers[i].Call("Raft.AppendEntries", args, reply)
	rf.PortPrintf("previndex :%d", args.PrevLogIndex)
	if reply.Accepted {
		logLength := rf.getLogLen()
		atomic.StoreInt64(&rf.nextIndex[i], int64(logLength))
		atomic.StoreInt64(&rf.matchIndex[i], int64(logLength-1))
		return
	}
	if args.Term == reply.Term && atomic.LoadInt64(&rf.nextIndex[i]) > 0 {
		atomic.AddInt64(&rf.nextIndex[i], -1)
	}
	if reply.Term > atomic.LoadInt64(latestTerm) {
		atomic.StoreInt64(latestTerm, reply.Term)
	}
}

func (rf *Raft) sendHeartBeat() {
	if atomic.LoadInt32(&rf.state) != LEADER {
		return
	}
	rf.leaderCommit()
	latestTerm := atomic.LoadInt64(&rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, &latestTerm)

	}
	time.Sleep(rf.heartbeatTimeout / 3)
	if atomic.LoadInt64(&latestTerm) > atomic.LoadInt64(&rf.currentTerm) && atomic.LoadInt32(&rf.state) == LEADER {
		atomic.StoreInt32(&rf.state, FOLLOWER)
		atomic.StoreInt64(&rf.currentTerm, latestTerm)
	}
}

func (rf *Raft) startElection() bool {
	args := &RequestVoteArgs{}
	args.CandidateId = rf.me
	atomic.CompareAndSwapInt32(&rf.state, FOLLOWER, CANDIDATE)
	Assert(atomic.LoadInt32(&rf.state) == CANDIDATE, "Wrong State")
	currentTerm := atomic.AddInt64(&rf.currentTerm, 1)
	rf.PortPrintf("new election, term %d", currentTerm)
	args.Term = currentTerm
	args.LastLogIndex = rf.getLogLen() - 1
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.log.get(args.LastLogIndex).Term
	}
	rf.votedFor = rf.me
	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, votes *int32) {
			reply := &RequestVoteReply{}
			if rf.peers[i].Call("Raft.RequestVote", args, reply) {
				if reply.VoteGranted {
					atomic.AddInt32(votes, 1)
				}
			}
		}(i, &votes)
	}
	time.Sleep(rf.heartbeatTimeout)
	if int(atomic.LoadInt32(&votes)) > len(rf.peers)/2 && atomic.CompareAndSwapInt32(&rf.state, CANDIDATE, LEADER) {
		logLength := rf.getLogLen()
		for i := 0; i < len(rf.peers); i++ {
			atomic.StoreInt64(&rf.nextIndex[i], int64(logLength))
			atomic.StoreInt64(&rf.matchIndex[i], -1)
		}
		rf.PortPrintf("become the new leader")

		return true
	}
	return false
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := (atomic.LoadInt32(&rf.state) == LEADER)
	term := atomic.LoadInt64(&rf.currentTerm)
	if !isLeader {
		return -1, int(term), isLeader
	}
	index := rf.getLogLen()
	newLog := LogEntrie{
		Index:   index,
		Term:    atomic.LoadInt64(&rf.currentTerm),
		Command: command,
	}
	rf.log.append([]LogEntrie{newLog})
	if rf.getLogLen() != newLog.Index+1 {
		panic("append error")
	}
	rf.PortPrintf("start %d", index)
	//tester index start from 1
	return index + 1, int(term), isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if atomic.LoadInt32(&rf.state) == LEADER {
			time.Sleep(rf.heartbeatTimeout)
			rf.mu.Lock()
			rf.sendHeartBeat()
			rf.mu.Unlock()
		}
		if atomic.LoadInt32(&rf.state) != LEADER {
			select {
			case <-time.After(rf.electionTimeout):
				// rf.PortPrintf("lost connect with leader, term %d", atomic.LoadInt64(&rf.currentTerm))
				rf.mu.Lock()
				if rf.startElection() {
					rf.sendHeartBeat()
				}
				rf.mu.Unlock()
			case <-rf.msgCh:
			}
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.nextIndex = make([]int64, len(peers))
	rf.matchIndex = make([]int64, len(peers))
	rf.commitIndex = -1
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.msgCh = make(chan LogEntrie)
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.heartbeatTimeout = 100 * time.Millisecond
	rf.electionTimeout = time.Duration(rand.Intn(100*10)+100*5) * time.Millisecond

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.PortPrintf("port start")

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
