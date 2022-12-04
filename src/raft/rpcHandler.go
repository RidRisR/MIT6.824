package raft

import (
	"sync/atomic"
)

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
	Term      int64
	Accepted  bool
	LastTerm  int64
	LastIndex int
}

type LogEntrie struct {
	Index   int
	Term    int64
	Command interface{}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// TODO:Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < reply.Term {
		rf.PortPrintf("%d lower term", args.CandidateId)
		return
	}

	if args.Term > reply.Term {
		atomic.CompareAndSwapInt64(&rf.currentTerm, reply.Term, args.Term)
		reply.Term = args.Term
		atomic.CompareAndSwapInt32(&rf.state, LEADER, FOLLOWER)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		rf.PortPrintf("have voted to %d", rf.votedFor)
		return
	}

	if rf.logGetLen() > 0 {
		lastLog := rf.logGetItem(-1)
		if lastLog.Term > args.LastLogTerm {
			rf.PortPrintf("not latest log")
			return
		} else if lastLog.Term == args.LastLogTerm && lastLog.Index > args.LastLogIndex {
			rf.PortPrintf("not longest log")
			return
		}
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	go rf.resetTimer()
	rf.PortPrintf("vote to %d", rf.votedFor)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Accepted = false

	if args.Term < reply.Term {
		// rf.PortPrintf("wrong term %d,%d", args.Term, reply.Term)
		return
	}

	if rf.state != FOLLOWER || rf.currentTerm != args.Term {
		atomic.StoreInt32(&rf.state, FOLLOWER)
		atomic.StoreInt64(&rf.currentTerm, args.Term)
	}
	go rf.resetTimer()

	reply.LastIndex, reply.LastTerm = rf.getLastConsensus(args.PrevLogIndex, args.PrevLogTerm)
	if reply.LastIndex != args.PrevLogIndex || reply.LastTerm != args.PrevLogTerm {
		rf.PortPrintf("no consensus %d,%d != %d,%d", args.PrevLogIndex, args.PrevLogTerm, reply.LastIndex, reply.LastTerm)
		return
	}
	rf.PortPrintf("consensus to %d: %d,%d (%d) = %d,%d (%d)", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, reply.LastIndex, reply.LastTerm, rf.lastAppliedIndex)

	if args.Type == LOG {
		rf.logAppend(args.Entries)
	}
	reply.Accepted = true
	rf.updateCommit(args.LeaderCommit)
}

func (rf *Raft) updateCommit(leaderCommit int) {
	if rf.commitIndex >= leaderCommit {
		return
	}

	var commitTo int
	lastIndex := rf.logGetLen() - 1
	if lastIndex < leaderCommit {
		commitTo = lastIndex
	} else {
		commitTo = leaderCommit
	}
	go rf.apply(commitTo)
	rf.PortPrintf("commit updat %d", commitTo)
	rf.commitIndex = commitTo
}

func (rf *Raft) resetTimer() {
	rf.msgCh <- LogEntrie{}
}

func (rf *Raft) apply(end int) {
	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()
	if rf.lastAppliedIndex >= end {
		return
	}
	if rf.logGetLen()-1 < end {
		rf.PortPrintf("wrong end %d,%d", rf.logGetLen()-1, end)
	}
	for _, log := range rf.logSlice(rf.lastAppliedIndex+1, end+1) {
		rf.PortPrintf("commit: %d,%v", log.Index, log.Command)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index + 1,
		}
	}
	rf.lastAppliedIndex = end
}
