package gomrjob

import (
	"fmt"
	"os"
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
