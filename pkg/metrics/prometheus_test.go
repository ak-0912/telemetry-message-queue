package metrics

import (
	"testing"
	"time"
)

func TestNewRegistry(t *testing.T) {
	reg, m := NewRegistry()
	if reg == nil || m == nil {
		t.Fatal()
	}
	m.IncPublished("t", 1, 2)
	m.IncFetched("g", 2, 3)
	m.IncRebalance()
	m.SetLag("g", "t", 0, 1.5)
	m.SetPartitionSize("t", 0, 42)
	m.ObservePublishLatency("t", 0, time.Millisecond)
}

func TestMQ_nilReceiver_noPanic(t *testing.T) {
	var m *MQ
	m.ObservePublishLatency("t", 0, time.Millisecond)
	m.IncPublished("t", 0, 1)
	m.IncFetched("g", 0, 1)
	m.IncRebalance()
	m.SetLag("g", "t", 0, 1)
	m.SetPartitionSize("t", 0, 1)
}

func TestMQ_IncPublished_zeroNoOp(t *testing.T) {
	_, m := NewRegistry()
	m.IncPublished("t", 0, 0)
	m.IncFetched("g", 0, 0)
}
