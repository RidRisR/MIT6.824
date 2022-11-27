package raft

import "sync/atomic"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Type         string
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntrie
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int64
	Accepted     bool
	LastLogIndex int64
}

type LogEntrie struct {
	Leader  int
	Term    int64
	Command interface{}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO:Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	if args.Term < reply.Term {
		rf.PortPrintf("%d lower term", args.CandidateId)
		return
	}

	if args.Term > reply.Term {
		atomic.CompareAndSwapInt64(&rf.currentTerm, reply.Term, args.Term)
		reply.Term = args.Term
		atomic.StoreInt32(&rf.state, FOLLOWER)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		rf.PortPrintf("have voted to %d", rf.votedFor)
		return
	}

	if rf.commitIndex > args.LastLogIndex || rf.commitTerm > args.LastLogTerm {
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	go rf.resetTimer()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	reply.LastLogIndex = int64(len(rf.log) - 1)
	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	//3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	//4. Append any new entries not already in the log
	//5. If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.Term < reply.Term {
		reply.Accepted = false
		return
	}

	if (len(rf.log)) <= args.PrevLogIndex {
		reply.Accepted = false
		return
	}

	if rf.log[args.PrevLogIndex].Term > args.Term {
		reply.Accepted = false
		return
	}

	atomic.StoreInt32(&rf.state, FOLLOWER)
	go rf.resetTimer()

	if (len(rf.log)) > (args.PrevLogIndex+1) && rf.log[args.PrevLogIndex+1].Term != args.Term {
		rf.log = rf.log[:args.PrevLogIndex]
	}

	if args.Type == LOG {
		rf.log = append(rf.log, args.Entries...)
	}

	if rf.commitIndex < args.LeaderCommit {
		if len(rf.log)-1 < args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Accepted = true
}

func (rf *Raft) resetTimer() {
	rf.msgCh <- LogEntrie{}
}
