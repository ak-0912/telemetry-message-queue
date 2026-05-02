package config

import (
	"testing"
	"time"
)

func TestLoad_overrides(t *testing.T) {
	t.Setenv("MQ_GRPC_PORT", "6000")
	t.Setenv("MQ_HTTP_PORT", "6080")
	t.Setenv("MQ_PARTITION_COUNT", "128")
	t.Setenv("MQ_DATA_DIR", "/tmp/mq")
	t.Setenv("MQ_RETENTION_HOURS", "48")
	t.Setenv("MQ_HEARTBEAT_TIMEOUT_SEC", "20")
	t.Setenv("MQ_MAX_PARTITION_SIZE", "5000")
	t.Setenv("MQ_FETCH_BATCH_DEFAULT", "64")
	t.Setenv("LOG_LEVEL", "debug")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.GRPCPort != 6000 || c.HTTPPort != 6080 {
		t.Fatalf("ports %d %d", c.GRPCPort, c.HTTPPort)
	}
	if c.PartitionCount != 128 || c.DataDir != "/tmp/mq" {
		t.Fatalf("partition/data %+v", c)
	}
	if c.Retention != 48*time.Hour || c.HeartbeatTimeoutSec != 20 {
		t.Fatalf("retention/heartbeat %+v", c)
	}
	if c.MaxPartitionSize != 5000 || c.FetchBatchDefault != 64 {
		t.Fatalf("sizes %+v", c)
	}
	if c.LogLevel != "debug" {
		t.Fatalf("log %q", c.LogLevel)
	}
}

func TestLoad_retentionHours_nonPositive_clamped(t *testing.T) {
	t.Setenv("MQ_RETENTION_HOURS", "0")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Retention != 24*time.Hour {
		t.Fatalf("got %v", c.Retention)
	}
}

func TestLoad_logLevel_variants(t *testing.T) {
	for _, lvl := range []string{"warn", "warning", "error", "DEBUG"} {
		t.Run(lvl, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", lvl)
			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if c.LogLevel == "" {
				t.Fatal("empty log level")
			}
		})
	}
}
