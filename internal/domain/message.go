package domain

import "time"

// Message is a single log record in a partition.
type Message struct {
	Partition   int32
	Offset      int64
	Key         string
	Payload     []byte
	PublishedAt time.Time
}
