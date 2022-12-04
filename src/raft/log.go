package raft

import (
	"sync"
)

type Log struct {
	mu   sync.Mutex
	data []LogEntrie
	len  int
}

func (l *Log) append(logs []LogEntrie) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, log := range logs {
		if log.Index == l.len {
			l.data = append(l.data, log)
			l.len++
			continue
		}
		if log.Command != l.data[log.Index].Command {
			l.data = l.data[:log.Index]
			l.data = append(l.data, log)
			l.len = log.Index + 1
		}
	}
}

func (l *Log) slice(start int, end int) []LogEntrie {
	l.mu.Lock()
	defer l.mu.Unlock()
	if end == -1 {
		return l.data[start:]
	}
	return l.data[start:end]
}

func (l *Log) get(index int) LogEntrie {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index == -1 {
		return l.data[l.len]
	}
	return l.data[index]
}

func (l *Log) getLastConsensus(index int, term int64) (lastIndex int, lastTerm int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < 0 || term <= 0 || l.len <= 0 {
		return -1, 0
	}
	if l.len > index && l.data[index].Term == term {
		return index, term
	}
	startIndex := index
	startTerm := term
	if l.len <= index {
		startIndex = l.len - 1
		startTerm = l.data[startIndex].Term
	}
	for i := startIndex; i > 0; i-- {
		if l.data[i].Term < startTerm {
			return i, l.data[i].Term
		}
	}
	return -1, 0
}

func (l *Log) logLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.len
}

// unused
// func (l *Log) cutTo(index int) error {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()
// 	if l.len == 0 {
// 		return nil
// 	}
// 	if index < 0 || l.len < index {
// 		return errors.New("illegal index")
// 	}
// 	l.data = l.data[:index]
// 	l.len = index
// 	return nil
// }

//wrapper functions

func (rf *Raft) logGetLen() int {
	return rf.log.logLen()
}

func (rf *Raft) logSlice(start int, end int) []LogEntrie {
	return rf.log.slice(start, end)
}

func (rf *Raft) logGetItem(index int) LogEntrie {
	return rf.log.get(index)
}

func (rf *Raft) logAppend(logs []LogEntrie) {
	rf.log.append(logs)
	rf.persist()
}

func (rf *Raft) getLastConsensus(index int, term int64) (lastIndex int, lastTerm int64) {
	return rf.log.getLastConsensus(index, term)
}

// unused
// func (rf *Raft) logCutTo(index int) {
// 	if err := rf.log.cutTo(index); err != nil {
// 		rf.PortPrintf(err.Error())
// 	} else {
// 		rf.persist()
// 	}
// }
