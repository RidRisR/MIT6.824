package raft

import (
	"sync"
)

type Log struct {
	mu   sync.Mutex
	data []LogEntrie
	len  int
}

func (l *Log) append(logs []LogEntrie) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, log := range logs {
		if log.Index == l.len {
			l.data = append(l.data, log)
			l.len++
		}
	}

	return true
}

func (l *Log) removeTo(index int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if index < 0 || l.len < index {
		return false
	}
	l.data = l.data[:index]
	l.len = index
	if l.len != len(l.data) {
		panic("length error")
	}
	return true
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

func (l *Log) logLen() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.len
}
