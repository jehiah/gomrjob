package gomrjob

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestListen(t *testing.T) {
	addr := startRemoteLogListner()
	assert.NotEqual(t, addr, "0.0.0.0:0")
}
