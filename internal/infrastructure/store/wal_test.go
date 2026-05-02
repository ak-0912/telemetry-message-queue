package store

import (
	"path/filepath"
	"testing"
	"time"

	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"google.golang.org/protobuf/proto"
)

func TestWalRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "0.wal")

	w, err := OpenWal(path)
	if err != nil {
		t.Fatal(err)
	}
	msgs := []*mqv1.Message{
		{Partition: 0, Offset: 0, Key: "GPU-a", Payload: []byte(`{"x":1}`), PublishedAt: time.Date(2025, 7, 18, 0, 0, 0, 0, time.UTC).UnixNano()},
		{Partition: 0, Offset: 1, Key: "GPU-b", Payload: []byte(`{"x":2}`), PublishedAt: time.Date(2025, 7, 18, 0, 0, 1, 0, time.UTC).UnixNano()},
	}
	for _, m := range msgs {
		if err := w.Append(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []*mqv1.Message
	if err := ReplayWal(path, func(m *mqv1.Message) error {
		cp := proto.Clone(m).(*mqv1.Message)
		got = append(got, cp)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(msgs) {
		t.Fatalf("len got %d want %d", len(got), len(msgs))
	}
	for i := range msgs {
		if !proto.Equal(got[i], msgs[i]) {
			t.Fatalf("record %d mismatch:\n got %+v\nwant %+v", i, got[i], msgs[i])
		}
	}
}

func TestReplayWal_missingFile(t *testing.T) {
	t.Parallel()
	err := ReplayWal(filepath.Join(t.TempDir(), "nope.wal"), func(*mqv1.Message) error {
		t.Fatal("callback should not run")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
