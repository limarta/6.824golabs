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
	dInit            logTopic = "INIT"
	dLeader          logTopic = "LEAD"
	dStart           logTopic = "START"
	dReqVote         logTopic = "REQVOTE"
	dTimer           logTopic = "TIMER"
	dElect           logTopic = "ELECT"
	dBeat            logTopic = "BEAT"
	dTick            logTopic = "TICK"
	dWon             logTopic = "WON"
	dAppend          logTopic = "AE"
	dAppendListen    logTopic = "AL"
	dDemote          logTopic = "DEMOTE"
	dCommit          logTopic = "COMMIT"
	dApply           logTopic = "APPLY_R"
	dLoss            logTopic = "LOST"
	dLogs            logTopic = "LOGS"
	dStartAccept     logTopic = "START"
	dCommit2         logTopic = "COMMIT2"
	dDecreaseIndex   logTopic = "DECINDEX"
	dBeat2           logTopic = "BEAT2"
	dIgnore          logTopic = "IGNORE"
	dNewTerm         logTopic = "NEWTERM"
	dRead            logTopic = "READ"
	dPersist         logTopic = "PERSIST"
	dConflict        logTopic = "CONFLICT"
	dStale           logTopic = "STALE"
	dSnapshot        logTopic = "SNAP_R"
	dSnapshotApplied logTopic = "SNAPAPP"
	dInstall         logTopic = "INSTALL"
	dSearchCommit    logTopic = "SEARCH"
	dCut             logTopic = "CUT_R"
)

var debug_2 map[logTopic]int = map[logTopic]int{dStart: 1, dWon: 1, dSearchCommit: 1, dPersist: 1, dApply: 1, dConflict: 1}
var debug_4 map[logTopic]int = map[logTopic]int{dCut: 1, dInstall: 1, dSnapshot: 1}
var debug_5 map[logTopic]int = map[logTopic]int{dCut: 1, dInstall: 1, dSnapshot: 1, dApply: 1, dStart: 1, dAppend: 1}

// Debugging
const Debug = false

var debugVerbosity int
var debugStart time.Time

func SetVerbosity() int {
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
	fmt.Println("RAFT VERBOSITY: ", level)
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	return level
}

func resetVerbosity() int {
	debugVerbosity = 0
	return 0
}

func DPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	time := time.Since(debugStart).Microseconds()
	prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	format = prefix + format
	if debugVerbosity == 4 {
		log.Printf(format, a...)
	} else if debugVerbosity == 2 {
		if _, ok := debug_2[dTopic]; ok {
			log.Printf(format, a...)
		}
	} else if debugVerbosity == 3 {
		if dTopic == dPersist || dTopic == dRead {
			log.Printf(format, a...)
		}
	} else if debugVerbosity == 1 {
		if _, ok := debug_5[dTopic]; ok {
			log.Printf(format, a...)
		}
	}
	return
}

func FPrintf(dTopic logTopic, format string, a ...interface{}) (n int, err error) {
	time := time.Since(debugStart).Microseconds()
	prefix := fmt.Sprintf("%06d %-7v ", time, string(dTopic))
	format = prefix + format
	log.Printf(format, a...)
	return
}
