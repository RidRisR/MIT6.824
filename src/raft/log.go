package raft

import (
	"errors"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	data    []LogEntrie
	termMap map[int64]int
	len     int
}

func (l *Log) append(logs []LogEntrie) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, log := range logs {
		if log.Index == l.len {
			l.data = append(l.data, log)
			l.len++
			if _, ok := l.termMap[log.Term]; !ok {
				l.termMap[log.Term] = log.Index
			}
		}
	}
}

func (l *Log) cutTo(index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.len == 0 {
		return nil
	}
	if index < 0 || l.len < index {
		return errors.New("illegal index")
	}
	lastTerm := l.data[l.len-1].Term
	l.data = l.data[:index]
	l.len = index

	if l.len == 0 {
		l.termMap = make(map[int64]int)
		return nil
	}
	for i := l.data[l.len-1].Term + 1; i < lastTerm+1; i++ {
		delete(l.termMap, i)
	}
	return nil
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

func (l *Log) getLastConsensus(term int64) (lastTerm int64, index int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.len == 0 {
		return 0, -1
	}
	for i := term; i > 0; i-- {
		if j, ok := l.termMap[i]; ok {
			return i, j
		}
	}
	return 0, -1
}

func (l *Log) logLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.len
}

//wrapper

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

func (rf *Raft) logCutTo(index int) {
	if err := rf.log.cutTo(index); err != nil {
		rf.PortPrintf(err.Error())
	} else {
		rf.persist()
	}
}
