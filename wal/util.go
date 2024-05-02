package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

func CreateSegmentFile(dir string, id int) (*os.File, error) {
	name := filepath.Join(dir, fmt.Sprintf("%s%d", SegmentPrefix, id))
	file, err := os.Create(name)

	if err != nil {
		return nil, err
	}

	return file, nil
}

func UnmarshalAndVerifyEntry(data []byte) (*Entry, error) {
	var entry Entry
	MustUnmarshal(data, &entry)

	if !VerifyCRC(&entry) {
		return nil, fmt.Errorf("CRC mismatch: data may be corrupted")
	}

	return &entry, nil
}

// Validates whether the given entry has a valid CRC.
func VerifyCRC(entry *Entry) bool {
	// Reset the entry CRC for the verification.
	actualCRC := crc32.ChecksumIEEE(append(entry.GetData(), byte(entry.GetLogSequenceNumber())))

	return entry.CRC == actualCRC
}

func MustMarshal(entry *Entry) []byte {
	marshaledEntry, err := proto.Marshal(entry)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}

	return marshaledEntry
}

func MustUnmarshal(data []byte, entry *Entry) {
	if err := proto.Unmarshal(data, entry); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}

func FindLastSegmentID(files []string) (int, error) {
	var lastID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		current, err := strconv.Atoi(strings.TrimPrefix(fileName, "segment-"))
		if err != nil {
			return 0, err
		}
		if current > lastID {
			lastID = current
		}
	}
	return lastID, nil
}
