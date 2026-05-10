package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cisco-interview/telemetry-message-queue/internal/application"
	"github.com/cisco-interview/telemetry-message-queue/internal/domain"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/coordinator"
	offsetstore "github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/offset"
	"github.com/cisco-interview/telemetry-message-queue/internal/infrastructure/store"
	grpcsvc "github.com/cisco-interview/telemetry-message-queue/internal/interface/grpc"
	"github.com/cisco-interview/telemetry-message-queue/pkg/config"
	"github.com/cisco-interview/telemetry-message-queue/pkg/metrics"
	mqv1 "github.com/cisco-interview/telemetry-message-queue/proto/mq/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)}))

	offsetDir := filepath.Join(cfg.DataDir, "offsets")
	offStore, err := offsetstore.NewFileOffsetStore(offsetDir)
	if err != nil {
		log.Error("offset store", "err", err)
		os.Exit(1)
	}

	partStore := store.NewMemoryPartitionStore(cfg.DataDir, cfg.PartitionCount, cfg.Retention, cfg.MaxPartitionSize, offStore)
	if err := partStore.EnsureTopic(domain.DefaultTopic, cfg.PartitionCount); err != nil {
		log.Error("ensure default topic", "err", err)
		os.Exit(1)
	}

	reg, m := metrics.NewRegistry()
	coord := coordinator.NewGroupCoordinator(cfg.PartitionCount, cfg.HeartbeatTimeoutSec, cfg.MaxGroupMembers, offStore, m.IncRebalance)

	publishUC := &application.PublishUsecase{
		Partitions:     partStore,
		PartitionCount: cfg.PartitionCount,
		Metrics:        m,
	}
	fetchUC := &application.FetchUsecase{
		Partitions:     partStore,
		PartitionCount: cfg.PartitionCount,
		Offsets:        offStore,
		Metrics:        m,
	}
	commitUC := &application.CommitOffsetUsecase{
		Partitions:     partStore,
		PartitionCount: cfg.PartitionCount,
		Offsets:        offStore,
	}
	coordUC := &application.CoordinatorUsecase{Coordinator: coord}

	srv := &grpcsvc.MessageQueueServer{
		Log:             log,
		Partitions:      partStore,
		PartitionCount:  cfg.PartitionCount,
		PublishUC:       publishUC,
		FetchUC:         fetchUC,
		CommitUC:        commitUC,
		CoordUC:         coordUC,
		FetchDefaultMax: cfg.FetchBatchDefault,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, 2*time.Second)
	go runRetention(ctx, partStore, 1*time.Minute)
	go runPartitionMetrics(ctx, partStore, m, 10*time.Second, cfg.PartitionCount)

	grpcLis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		log.Error("grpc listen", "err", err)
		os.Exit(1)
	}
	gs := grpc.NewServer()
	mqv1.RegisterMessageQueueServiceServer(gs, srv)
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	httpSrv := &http.Server{Addr: ":" + strconv.Itoa(cfg.HTTPPort), Handler: mux}

	go func() {
		log.Info("http listening", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server", "err", err)
		}
	}()

	go func() {
		log.Info("grpc listening", "addr", grpcLis.Addr().String())
		if err := gs.Serve(grpcLis); err != nil {
			log.Error("grpc server", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	cancel()
	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shCancel()
	_ = httpSrv.Shutdown(shCtx)
	gs.GracefulStop()
}

func runRetention(ctx context.Context, s *store.MemoryPartitionStore, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.RunRetentionGC(ctx)
		}
	}
}

func runPartitionMetrics(ctx context.Context, s *store.MemoryPartitionStore, m *metrics.MQ, every time.Duration, fallbackParts int) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, topic := range s.KnownTopics() {
				n := s.PartitionCount(topic)
				if n <= 0 {
					n = fallbackParts
				}
				for p := int32(0); p < int32(n); p++ {
					c := float64(s.MessageCount(topic, p))
					m.SetPartitionSize(topic, p, c)
				}
			}
		}
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
