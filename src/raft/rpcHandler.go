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
	Term     int64
	Accepted bool
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
	reply.VoteGranted = false
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
	rf.PortPrintf("vote to %d", rf.votedFor)
	go rf.resetTimer()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	reply.Accepted = false
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
		// rf.PortPrintf("wrong term %d,%d", args.Term, reply.Term)
		return
	}

	if (len(rf.log)) < args.PrevLogIndex+1 {
		rf.PortPrintf("wrong index %d>%d", args.PrevLogIndex+1, len(rf.log))
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term > args.Term {
		rf.PortPrintf("wrong log %d,%d", args.Term, rf.log[args.PrevLogIndex].Term)
		return
	}

	atomic.StoreInt32(&rf.state, FOLLOWER)
	go rf.resetTimer()

	if (len(rf.log)) > (args.PrevLogIndex+1) && rf.log[args.PrevLogIndex+1].Term != args.Term {
		if args.PrevLogIndex > 0 {
			rf.log = rf.log[:args.PrevLogIndex]
		} else {
			rf.log = rf.log[:0]
		}

	}

	if args.Type == LOG {
		rf.log = append(rf.log, args.Entries...)
	}
	rf.updateCommit(args)
	reply.Accepted = true
}

func (rf *Raft) updateCommit(args *AppendEntriesArgs) {
	if rf.commitIndex >= args.LeaderCommit {
		return
	}

	commitTo := rf.commitIndex
	if len(rf.log)-1 < args.LeaderCommit {
		commitTo = len(rf.log)
	} else {
		commitTo = args.LeaderCommit
	}
	for i := rf.commitIndex + 1; i < commitTo+1; i++ {
		rf.PortPrintf("comit:%d", i)
		go func(i int, v LogEntrie) {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      v.Command,
				CommandIndex: i + 1,
			}
		}(i, rf.log[i])
	}
	rf.commitIndex = commitTo
}

func (rf *Raft) resetTimer() {
	rf.msgCh <- LogEntrie{}
}