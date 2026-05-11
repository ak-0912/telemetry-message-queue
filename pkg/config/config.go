// Package config loads runtime configuration from environment variables.
//
// All settings use the MQ_ prefix (e.g. MQ_GRPC_PORT) except LOG_LEVEL,
// which is read without a prefix for compatibility with common logging
// conventions.
package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the runtime settings for the message queue server.
type Config struct {
	GRPCPort            int           // MQ_GRPC_PORT (default 50051)
	HTTPPort            int           // MQ_HTTP_PORT (default 8080)
	PartitionCount      int           // MQ_PARTITION_COUNT (default 256)
	DataDir             string        // MQ_DATA_DIR — root for WAL and offset files (default /data/mq)
	Retention           time.Duration // MQ_RETENTION_HOURS converted to time.Duration (default 24h)
	HeartbeatTimeoutSec int           // MQ_HEARTBEAT_TIMEOUT_SEC — stale-member eviction threshold (default 15)
	MaxGroupMembers     int           // MQ_MAX_GROUP_MEMBERS — cap per consumer group (default 10)
	MaxPartitionSize    int           // MQ_MAX_PARTITION_SIZE — retained messages per partition (default 100 000)
	LogLevel            string        // LOG_LEVEL — debug|info|warn|error (default info, no MQ_ prefix)
	FetchBatchDefault   int32         // MQ_FETCH_BATCH_DEFAULT — max messages per Fetch when client sends 0 (default 200)
}

// Load reads environment variables (with defaults) and returns a validated Config.
func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("MQ")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("GRPC_PORT", 50051)
	v.SetDefault("HTTP_PORT", 8080)
	v.SetDefault("PARTITION_COUNT", 256)
	v.SetDefault("DATA_DIR", "/data/mq")
	v.SetDefault("RETENTION_HOURS", 24)
	v.SetDefault("HEARTBEAT_TIMEOUT_SEC", 15)
	v.SetDefault("MAX_GROUP_MEMBERS", 10)
	v.SetDefault("MAX_PARTITION_SIZE", 100000)
	v.SetDefault("FETCH_BATCH_DEFAULT", 200)

	// LOG_LEVEL is bound without the MQ_ prefix.
	_ = v.BindEnv("LOG_LEVEL", "LOG_LEVEL")
	v.SetDefault("LOG_LEVEL", "info")

	retentionH := v.GetInt("RETENTION_HOURS")
	if retentionH <= 0 {
		retentionH = 24
	}

	return &Config{
		GRPCPort:            v.GetInt("GRPC_PORT"),
		HTTPPort:            v.GetInt("HTTP_PORT"),
		PartitionCount:      v.GetInt("PARTITION_COUNT"),
		DataDir:             v.GetString("DATA_DIR"),
		Retention:           time.Duration(retentionH) * time.Hour,
		HeartbeatTimeoutSec: v.GetInt("HEARTBEAT_TIMEOUT_SEC"),
		MaxGroupMembers:     v.GetInt("MAX_GROUP_MEMBERS"),
		MaxPartitionSize:    v.GetInt("MAX_PARTITION_SIZE"),
		LogLevel:            v.GetString("LOG_LEVEL"),
		FetchBatchDefault:   int32(v.GetInt("FETCH_BATCH_DEFAULT")),
	}, nil
}
