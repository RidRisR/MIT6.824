package raft

import "sync/atomic"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []LogEntrie
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int64
	Accepted bool
}

type LogEntrie struct {
	Leader  int
	Type    string
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

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	go rf.sendMsg(VOTE, args.CandidateId, args.Term)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//1. Reply false if term < currentTerm (§5.1)
	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	//5. If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.Term < atomic.LoadInt64(&rf.currentTerm) {
		reply.Accepted = false
		return
	}

	atomic.CompareAndSwapInt32(&rf.state, LEADER, FOLLOWER)
	go rf.sendMsg(HEARTBEAT, args.LeaderId, args.Term)
}

func (rf *Raft) sendMsg(msgType string, leader int, term int64) {
	if msgType == VOTE {
		rf.PortPrintf("%s: vote %d term %d", msgType, leader, term)
	}
	rf.msgCh <- LogEntrie{
		Type:   msgType,
		Leader: leader,
	}
}
