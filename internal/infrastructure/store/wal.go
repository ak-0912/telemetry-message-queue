package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/protobuf/proto"
)

// WalWriter appends length-prefixed protobuf mq.v1.Message records to an
// append-only file. Each record is [4-byte big-endian length][protobuf bytes]
// and is fsync'd after every write for durability.
type WalWriter struct {
	f *os.File
}

// OpenWal opens (or creates) a WAL file for appending. Parent directories
// are created automatically.
func OpenWal(path string) (*WalWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WalWriter{f: f}, nil
}

// Append serialises and writes a single record, followed by fsync.
func (w *WalWriter) Append(msg *mqv1.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(b); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes and closes the underlying file. Safe to call on a nil receiver.
func (w *WalWriter) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	return w.f.Close()
}

// ReplayWal reads WAL from path and invokes fn for each message in order.
func ReplayWal(path string, fn func(*mqv1.Message) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		n := binary.BigEndian.Uint32(hdr[:])
		if n > 1<<28 { // sanity
			return fmt.Errorf("wal: invalid record length %d", n)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(f, buf); err != nil {
			return err
		}
		var m mqv1.Message
		if err := proto.Unmarshal(buf, &m); err != nil {
			return err
		}
		if err := fn(&m); err != nil {
			return err
		}
	}
}
