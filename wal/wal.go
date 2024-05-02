package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	sync "sync"
	"time"
)

const (
	SyncInterval  = 200 * time.Millisecond
	SegmentPrefix = "segment-"
)

type WAL struct {
	directory           string
	currentSegment      *os.File
	lock                sync.Mutex
	lastSequenceNo      uint64
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int
	currentSegmentIndex int
	ctx                 context.Context
	cancel              context.CancelFunc
}

func OpenWAL(dir string, fsync bool, maxSize uint, maxSegment uint) (*WAL, error) {

	// create directory if not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, err
		}
	}

	files, err := filepath.Glob(filepath.Join(dir, SegmentPrefix+"*"))

	if err != nil {
		return nil, err
	}

	var lastSegmentID int = 0

	if len(files) > 0 {
		lastSegmentID, err = FindLastSegmentID(files)
		if err != nil {
			return nil, err
		}
	} else {
		// no files here
		file, err := CreateSegmentFile(dir, 0)
		if err != nil {
			return nil, err
		}

		if err := file.Close(); err != nil {
			return nil, err
		}

	}

	path := filepath.Join(dir, fmt.Sprintf("%s%d", SegmentPrefix, lastSegmentID))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)

	if err != nil {
		return nil, err
	}

	// seek to end file, because we want to append-it
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		directory:           dir,
		currentSegment:      file,
		lastSequenceNo:      0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(SyncInterval), // syncInterval is a predefined duration
		shouldFsync:         fsync,
		maxFileSize:         int64(maxSize),
		maxSegments:         int(maxSize),
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	// check last sequence number

	// keep the wal syncing
	go wal.KeepSyncing()

	return wal, nil

}

func (wal *WAL) Write(data []byte, checkpoint bool) error {

	wal.lock.Lock()

	defer wal.lock.Unlock()

	if checkpoint {
		// TODO: checkpoint, sync it first
		return nil
	}

	wal.lastSequenceNo++
	entry := &Entry{
		LogSequenceNumber: wal.lastSequenceNo,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(append(data, byte(wal.lastSequenceNo))),
	}

	return wal.BufferWrite(entry)
}

func (wal *WAL) KeepSyncing() {
	for {
		select {
		case <-wal.syncTimer.C:

			fmt.Printf("Getting timeout from SyncInterval \n")

			wal.lock.Lock()
			err := wal.Sync()
			wal.lock.Unlock()

			if err != nil {
				fmt.Printf("Error while performing sync: %v", err)
			}

		case <-wal.ctx.Done():
			return
		}
	}
}

func (wal *WAL) ResetTimer() {
	wal.syncTimer.Reset(SyncInterval)
}

func (wal *WAL) Sync() error {
	if err := wal.bufWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.currentSegment.Sync(); err != nil {
			return err
		}
	}

	// Reset the keepSyncing timer, since we just synced.
	wal.ResetTimer()

	return nil
}

// Function to write data
func (wal *WAL) BufferWrite(entry *Entry) error {
	marshaledEntry := MustMarshal(entry)

	size := int32(len(marshaledEntry))
	if err := binary.Write(wal.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	res, err := wal.bufWriter.Write(marshaledEntry)
	fmt.Println(res)
	return err
}
