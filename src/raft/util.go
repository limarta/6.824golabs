package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

type logTopic string

const (
	dInit    logTopic = "INIT"
	dLeader  logTopic = "LEAD"
	dStart   logTopic = "START"
	dReqVote logTopic = "REQVOTE"
	dTimer   logTopic = "TIMER"
	dElect   logTopic = "ELECT"
	dBeat    logTopic = "BEAT"
	dTick    logTopic = "TICK"
	dWon     logTopic = "WON"
)

// Debugging
const Debug = false

var debugVerbosity int

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
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

func resetVerbosity() int {
	debugVerbosity = 0
	return 0
}

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	if debugVerbosity >= 1 {
		prefix := fmt.Sprintf("%-7v ", string(dTopic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
