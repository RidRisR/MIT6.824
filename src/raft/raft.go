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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	heartbeatConst       = 60
	LEADER         int32 = 0
	FOLLOWER       int32 = 1
	CANDIDATE      int32 = 2
	HEARTBEAT            = "HTBT"
	VOTE                 = "VOTE"
	LOG                  = "LOG"
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
	nPeers      int
	currentTerm int64
	votedFor    int64
	log         Log

	//volatile
	commitIndex      int
	lastAppliedIndex int
	msgCh            chan AppendEntriesReply
	applyOpCh        chan int
	applyCh          chan ApplyMsg
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	state            int32

	//volatile for leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = int(rf.currentTerm)
	var isleader bool = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist(term int64, voteFor int64) {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(term)
	e.Encode(voteFor)
	rf.log.mu.Lock()
	e.Encode(rf.log.data)
	rf.log.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) deferPersist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.mu.Lock()
	e.Encode(rf.log.data)
	rf.log.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logData []LogEntrie
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	// d.Decode(&rf.lastAppliedIndex)
	if err := d.Decode(&logData); err == nil {
		rf.PortPrintf("reading log")
		rf.log.append(logData)
	} else {
		rf.PortPrintf(err.Error())
	}
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, votes *int64) {
	reply := RequestVoteReply{}
	if rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
		if reply.Term == args.Term && reply.VoteGranted {
			atomic.AddInt64(votes, 1)
		}
	}
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

func (rf *Raft) leaderCommit() int {
	if rf.logGetLen() == 0 {
		return -1
	}
	if rf.commitIndex+1 > rf.logGetLen() {
		rf.PortPrintf("!!!!! %d", rf.commitIndex)
	}
	uncommitted := rf.logSlice(rf.commitIndex+1, -1)
	oldCommitIndex := rf.commitIndex
	toCommit := -1
	for i, log := range uncommitted {
		count := 1
		for j := 0; j < rf.nPeers; j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= log.Index {
				count++
			}
		}
		if count <= rf.nPeers/2 {
			break
		}
		if log.Term == rf.currentTerm {

			toCommit = i
		}
	}
	if toCommit >= 0 {
		rf.commitIndex = uncommitted[toCommit].Index
	}
	return toCommit + oldCommitIndex + 1
}

func (rf *Raft) handleAppendEntries(reply *AppendEntriesReply) {
	if reply.Term < rf.currentTerm {
		// rf.PortPrintf("Term?! %d, %d", reply.Term, rf.currentTerm)
		return
	}
	i := reply.Peer
	if reply.Accepted {
		if reply.LastIndex >= rf.matchIndex[i] {
			rf.nextIndex[i] = reply.LastIndex + 1
			rf.matchIndex[i] = reply.LastIndex

		}
		// rf.PortPrintf("%v!!!! %d, %d", reply.Accepted, reply.LastIndex, rf.matchIndex[i])
		return
	}
	// rf.PortPrintf("%v!!!! %d, %d", reply.Accepted, reply.LastIndex, rf.matchIndex[i])
	if reply.LastIndex >= rf.matchIndex[i] {
		lastIndex, _ := rf.getLastConsensus(reply.LastIndex, reply.LastTerm)
		rf.nextIndex[i] = lastIndex + 1
	}
}

func (rf *Raft) sendAppendEntries(peer int, nextIndex int, args AppendEntriesArgs) {
	if rf.logGetLen()-1 >= int(nextIndex) {
		args.Type = LOG
		args.Entries = rf.logSlice(int(nextIndex), -1)
		args.PrevLogIndex = int(nextIndex) - 1
	}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.logGetItem(args.PrevLogIndex).Term
	}
	reply := AppendEntriesReply{}
	if rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) {
		rf.msgCh <- reply
	}
}

func (rf *Raft) sendHeartBeat(i int) {
	if rf.state != LEADER {
		return
	}
	args := AppendEntriesArgs{
		Type:         HEARTBEAT,
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.logGetLen() - 1,
	}
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, rf.nextIndex[i], args)
	}
}

func (rf *Raft) startElection() bool {
	rf.state = CANDIDATE
	// Assert(rf.state == CANDIDATE, "Wrong State")
	rf.currentTerm++
	rf.votedFor = int64(rf.me)
	go rf.persist(rf.currentTerm, rf.votedFor)
	rf.PortPrintf("new election, term %d", rf.currentTerm)
	var votes int64 = 1
	args := RequestVoteArgs{
		CandidateId:  rf.me,
		Term:         atomic.LoadInt64(&rf.currentTerm),
		LastLogIndex: rf.logGetLen() - 1,
	}
	if args.LastLogIndex >= 0 {
		args.LastLogTerm = rf.logGetItem(args.LastLogIndex).Term
	}
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, &votes)
	}

	for wait := 0; atomic.LoadInt64(&votes) <= int64(rf.nPeers/2) && wait < 15; wait++ {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt64(&votes) > int64(rf.nPeers/2) {
		rf.state = LEADER
		logLength := rf.logGetLen()
		for i := 0; i < rf.nPeers; i++ {
			rf.nextIndex[i] = logLength
			rf.matchIndex[i] = -1
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
	isLeader := (rf.state == LEADER)
	term := rf.currentTerm
	if !isLeader {
		return -1, int(term), isLeader
	}
	index := rf.logGetLen()
	newLog := LogEntrie{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logAppend([]LogEntrie{newLog})
	if rf.logGetLen() != newLog.Index+1 {
		panic("append error")
	}
	rf.PortPrintf("start %d,%v,%d", index, command, term)
	//tester index start from 1
	return index + 1, int(term), isLeader
}

func (rf *Raft) followerLoop() int64 {
	for !rf.killed() {
		select {
		case <-time.After(rf.electionTimeout):
			// rf.PortPrintf("lost connect with leader, term %d", rf.currentTerm)
			rf.mu.Lock()
			if rf.startElection() {
				rf.sendHeartBeat(0)
				rf.mu.Unlock()
				return rf.currentTerm
			}
			rf.mu.Unlock()
		case <-rf.msgCh:
		}
	}
	return 0
}

func (rf *Raft) leaderLoop(leaderTerm int64) {
	quit := false
	for !rf.killed() {
		time.Sleep(rf.heartbeatTimeout)
		rf.mu.Lock()
		if rf.state != LEADER || leaderTerm < rf.currentTerm || leaderTerm == 0 {
			quit = true
		}
		latestTerm := leaderTerm
		empty := false
		for {
			select {
			case reply := <-rf.msgCh:
				rf.handleAppendEntries(&reply)
			default:
				empty = true
			}
			if empty {
				break
			}
		}
		toApply := rf.leaderCommit()
		go rf.apply(toApply)
		// rf.PortPrintf("to commit %d", toApply)
		if latestTerm > leaderTerm {
			rf.state = FOLLOWER
			rf.currentTerm = latestTerm
			quit = true
		}
		go rf.persist(rf.currentTerm, rf.votedFor)
		if quit {
			rf.mu.Unlock()
			return
		}
		rf.sendHeartBeat(0)
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	state := FOLLOWER
	var leaderTerm int64 = 0
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		switch {
		case state == LEADER:
			rf.leaderLoop(leaderTerm)
			state = FOLLOWER
		case state == FOLLOWER:
			leaderTerm = rf.followerLoop()
			state = LEADER
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
	rf.nPeers = len(peers)
	rf.nextIndex = make([]int, rf.nPeers)
	rf.matchIndex = make([]int, rf.nPeers)
	rf.commitIndex = -1
	rf.lastAppliedIndex = -1
	rf.persister = persister
	rf.me = me
	rf.applyOpCh = make(chan int, 10)
	rf.applyCh = applyCh
	rf.msgCh = make(chan AppendEntriesReply, rf.nPeers*2)
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.heartbeatTimeout = heartbeatConst * time.Millisecond
	rf.electionTimeout = time.Duration(rand.Intn(heartbeatConst*15)+heartbeatConst*5) * time.Millisecond

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.PortPrintf("port ready")

	// start ticker goroutine to start elections
	go rf.waitForApply()
	go rf.ticker()

	return rf
}
