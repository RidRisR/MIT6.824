package raft

import (
	"fmt"
	"log"
	"sync/atomic"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var rolePrint = []string{"LDR", "FOW", "CAN"}

// Debugging
const Debug = true

func DPrintf(role int32, index int, format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf("[%s] %d:", rolePrint[role], index)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) PortPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf("[%s] %d:", rolePrint[atomic.LoadInt32(&rf.state)], rf.me)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

func Assert(assertion bool, format string, a ...interface{}) {
	if !assertion {
		msg := fmt.Sprintf(format, a...)
		panic(msg)
	}
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}
