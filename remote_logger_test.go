package gomrjob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListen(t *testing.T) {
	addr := startRemoteLogListner()
	assert.NotEqual(t, addr, "0.0.0.0:0")
}
