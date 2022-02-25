package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dInit         logTopic = "INIT"
	dLeader       logTopic = "LEAD"
	dStart        logTopic = "START"
	dReqVote      logTopic = "REQVOTE"
	dTimer        logTopic = "TIMER"
	dElect        logTopic = "ELECT"
	dBeat         logTopic = "BEAT"
	dTick         logTopic = "TICK"
	dWon          logTopic = "WON"
	dAppend       logTopic = "AE"
	dAppendListen logTopic = "AL"
	dDemote       logTopic = "DEMOTE"
	dCommit       logTopic = "COMMIT"
	dApply        logTopic = "APPLY"
)

// Debugging
const Debug = false

var debugVerbosity int
var debugStart time.Time

func setVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	debugVerbosity = level
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

func resetVerbosity() int {
	debugVerbosity = 0
	return 0
}

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	if debugVerbosity == 1 {
		time := time.Since(debugStart).Microseconds()
		prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
		format = prefix + format
		log.Printf(format, a...)
	} else if debugVerbosity == 2 {
		prefix := fmt.Sprintf("%-7v ", string(dTopic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
