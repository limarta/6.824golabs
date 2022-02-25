package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

type logTopic string

const (
	dLeader  logTopic = "LEAD"
	dStart   logTopic = "START"
	dReqVote logTopic = "REQVOTE"
	dTimer   logTopic = "TIMER"
	dElect   logTopic = "ELECT"
	dBeat    logTopic = "BEAT"
	dTick    logTopic = "TICK"
)

// Debugging
const Debug = false

var debugVerbosity int

func getVerbosity() int {
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
func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	if debugVerbosity >= 1 {
		prefix := fmt.Sprintf("%v ", string(dTopic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
