package gomrjob

import (
	"github.com/bmizerany/assert"
	"testing"
	"bytes"
)

func TestLs(t *testing.T) {
	shuntData := `
Found 1 items
-rw-r--r--   3 jehiah supergroup     176572 2013-09-06 15:23 hdfs:///user/jehiah/tmp/mrjob/a.jehiah.20130906.141932.492122/step-output/1/part-00008
Found 1 items
-rw-r--r--   3 jehiah supergroup     175856 2013-09-06 15:23 hdfs:///user/jehiah/tmp/mrjob/a.jehiah.20130906.141932.492122/step-output/1/part-00009
`

	out := make(chan *HdfsFile)
	go parseLsOutput(bytes.NewBufferString(shuntData), out)
	f1 := <- out
	assert.Equal(t, f1.ReplicaCount, int64(3))
	assert.Equal(t, f1.User, "jehiah")
	assert.Equal(t, f1.Size, int64(176572))
	assert.Equal(t, f1.Path, "hdfs:///user/jehiah/tmp/mrjob/a.jehiah.20130906.141932.492122/step-output/1/part-00008")
	f2 := <- out
	assert.Equal(t, f2.Path, "hdfs:///user/jehiah/tmp/mrjob/a.jehiah.20130906.141932.492122/step-output/1/part-00009")

}
