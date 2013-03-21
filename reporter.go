package gomrjob

import (
	"fmt"
	"log"
	"os"
	"syscall"
	"time"
)

// reporter:counter:<group>,<counter>,<amount>
func Counter(group string, counter string, amount int64) {
	fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,%d\n", group, counter, amount)
	os.Stderr.Sync()
}

//  reporter:status:<message>
func Status(message string) {
	fmt.Fprintf(os.Stderr, "reporter:status:%s\n", message)
	os.Stderr.Sync()
}

func auditCpuTime(group string, prefix string) {
	var u syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &u)
	if err != nil {
		log.Printf("error getting Rusage: %s", err)
		return
	}
	userTime := time.Duration(u.Utime.Nano()) * time.Nanosecond
	systemTime := time.Duration(u.Stime.Nano()) * time.Nanosecond
	Counter(group, fmt.Sprintf("%s userTime (ms)", prefix), int64(userTime/time.Millisecond))
	Counter(group, fmt.Sprintf("%s systemTime (ms)", prefix), int64(systemTime/time.Millisecond))
}
