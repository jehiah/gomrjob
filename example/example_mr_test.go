package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/jehiah/gomrjob/mrtest"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	step := &JsonEntryCounter{}
	in := `{"key_field":"z"}
			{"key_field":"a"}
			{"key_field":"another"}
			{"key_field":"z"}
			{"key_field":"z"}
			{"another_key":"a"}
`
	out := `"another_key"	1
"key_field"	5
"lines_read"	6
`
	mrtest.TestMapReduceStep(t, step, bytes.NewBufferString(in), bytes.NewBufferString(out))
}
