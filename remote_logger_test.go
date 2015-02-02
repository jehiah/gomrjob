package gomrjob

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestListen(t *testing.T) {
	addr := startRemoteLogListner()
	assert.NotEqual(t, addr, "0.0.0.0:0")
}
