package raft

import (
	"log"
	"os"
	"strconv"
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
	return level
}
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debugVerbosity >= 1 {
		log.Printf(format, a...)
	}
	return
}
