package raft

import (
	"errors"
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
		}
	}
}

func (l *Log) cutTo(index int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < 0 || l.len < index {
		return errors.New("illegal index")
	}
	l.data = l.data[:index]
	l.len = index
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

//wrapper

func (l *Log) logLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.len
}

func (rf *Raft) logGetLen() int {
	return rf.log.logLen()
}

func (rf *Raft) logSlice(start int, end int) []LogEntrie {
	return rf.log.slice(start, end)
}

func (rf *Raft) logPointRead(index int) LogEntrie {
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
