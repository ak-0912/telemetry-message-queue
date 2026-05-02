package store

import "errors"

var (
	// ErrUnknownTopic is returned when a topic has not been created yet.
	ErrUnknownTopic = errors.New("unknown topic")
	// ErrInvalidPartition is returned when partition id is out of range.
	ErrInvalidPartition = errors.New("invalid partition")
)
