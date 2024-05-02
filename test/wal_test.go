package test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	wal "nexteam.id/gowal/wal"
)

func TestWAL_Basic(t *testing.T) {

	t.Parallel()
	// Setup: Create a temporary file for the WAL
	dirPath := "data"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	db, err := wal.OpenWAL(dirPath, true, 4*1024, 1024)

	assert.NoError(t, err, "Failed to open WAL (create directory, and create first segment file)")

	entries := []wal.Record{
		{Key: "key1", Value: []byte("value1"), Op: wal.InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: wal.InsertOperation},
		{Key: "key3", Op: wal.DeleteOperation},
	}

	for _, entry := range entries {
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, db.Write(marshaledEntry, false), "Failed to write entry")
	}

}
