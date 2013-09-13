package main

import (
	"bytes"
	"github.com/jehiah/gomrjob"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	step := &JsonEntryCounter{"key_field"}
	in := `{"key_field":"a"}
			{"key_field":"z"}
			{"key_field":"another"}
			{"key_field":"z"}
			{"key_field":"z"}
			{"key_field":"a"}`
	out := `"a"	2
"another"	1
"z"	3
`
	gomrjob.TestMapReduceStep(t, step, bytes.NewBufferString(in), bytes.NewBufferString(out))
}
