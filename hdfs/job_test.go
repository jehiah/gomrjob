package hdfs

import (
	"fmt"
	"testing"
)

func TestAbsolutePath(t *testing.T) {
	type testCase struct {
		path, proto, expect string
	}
	tests := []testCase{
		{"/a", "hdfs:///", "hdfs:///a"},
		{"a", "hdfs:///", "hdfs:///a"},
		{"/a", "", "hdfs:///a"},
		{"hdfs:///a", "gs://bucket/", "hdfs:///a"},
		{"gs://bucketa/a", "gs://bucketb/", "gs://bucketa/a"},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			got := absolutePath(tc.path, tc.proto)
			if got != tc.expect {
				t.Errorf("got %q expected %q for path %q proto %q", got, tc.expect, tc.path, tc.proto)
			}
		})
	}
}
