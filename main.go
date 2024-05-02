package main

import (
	"encoding/json"
	"time"

	"nexteam.id/gowal/wal"
)

func main() {

	db, err := wal.OpenWAL("data", true, 4*1024, 1024)

	if err != nil {
		panic(err)
	}

	entry := wal.Record{Key: "key1", Value: []byte("value1"), Op: wal.InsertOperation}

	marshal, err := json.Marshal(entry)

	if err != nil {
		panic(err)
	}

	if err = db.Write(marshal, false); err != nil {
		panic(err)
	}

	if err = db.Write(marshal, false); err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)

}
