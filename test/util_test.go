package test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	wal "nexteam.id/gowal/wal"
)

func Test_CreateSegment(t *testing.T) {
	os.MkdirAll("test-segment", 0775)
	_, err := wal.CreateSegmentFile("test-segment", 10)
	assert.NoError(t, err, "Failed to create segment file")
}
