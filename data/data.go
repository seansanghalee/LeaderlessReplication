package data

import "time"

// Data is a data struct that is sent between clients and servers that contains key-value pair with a timestamp
type Data struct {
	Key, Value  string
	Timestamp   time.Time
	Ack         int // 1 for ACK back to client/server, null otherwise
	Exists      int // if <k,v> pair exists or not
	ReadOrWrite int // 0 for read, 1 for write
}
