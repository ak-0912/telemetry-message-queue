package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds runtime configuration from environment variables.
type Config struct {
	GRPCPort            int
	HTTPPort            int
	PartitionCount      int
	DataDir             string
	Retention           time.Duration
	HeartbeatTimeoutSec int
	MaxGroupMembers     int
	MaxPartitionSize    int
	LogLevel            string
	FetchBatchDefault   int32
}

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
	_ = v.BindEnv("LOG_LEVEL", "LOG_LEVEL")
	v.SetDefault("LOG_LEVEL", "info")
	logLevel := v.GetString("LOG_LEVEL")

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
		LogLevel:            logLevel,
		FetchBatchDefault:   int32(v.GetInt("FETCH_BATCH_DEFAULT")),
	}, nil
}
