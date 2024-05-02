## GoWal

simple exploration about WAL (Write Ahead Log) in Golang

## TIL

- You must flush buffer writer to write it to file, since the goals of WAL is high write performance flush will be periodically or by trigger it (02/05/2024)
- Command to generate `.pb.go` for marshal / unmarshal entry to file (02/05/2024)
```sh
protoc --go_out=. --go_opt=paths=source_relative type.proto
```