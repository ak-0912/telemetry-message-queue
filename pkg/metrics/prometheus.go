package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MQ exposes Prometheus collectors for the message queue service.
type MQ struct {
	Published      *prometheus.CounterVec
	Fetched        *prometheus.CounterVec
	Rebalance      prometheus.Counter
	Lag            *prometheus.GaugeVec
	PartitionSize  *prometheus.GaugeVec
	PublishLatency *prometheus.HistogramVec
}

// NewRegistry creates a dedicated registry with MQ metrics registered.
func NewRegistry() (*prometheus.Registry, *MQ) {
	reg := prometheus.NewRegistry()
	m := &MQ{
		Published: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mq_messages_published_total",
			Help: "Total messages published by topic and partition.",
		}, []string{"topic", "partition"}),
		Fetched: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mq_messages_fetched_total",
			Help: "Total messages fetched by consumer group and partition.",
		}, []string{"group", "partition"}),
		Rebalance: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mq_rebalance_total",
			Help: "Number of consumer group rebalances.",
		}),
		Lag: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "mq_consumer_lag",
			Help: "Approximate offset lag by group, topic, and partition.",
		}, []string{"group", "topic", "partition"}),
		PartitionSize: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "mq_partition_size",
			Help: "Current retained messages per topic and partition.",
		}, []string{"topic", "partition"}),
		PublishLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "mq_publish_latency_seconds",
			Help:    "Publish handler latency in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"topic", "partition"}),
	}
	return reg, m
}

func (m *MQ) ObservePublishLatency(topic string, partition int32, d time.Duration) {
	if m == nil {
		return
	}
	p := prometheus.Labels{
		"topic": topic, "partition": itoa(int(partition)),
	}
	m.PublishLatency.With(p).Observe(d.Seconds())
}

func (m *MQ) IncPublished(topic string, partition int32, n int64) {
	if m == nil || n == 0 {
		return
	}
	m.Published.With(prometheus.Labels{
		"topic": topic, "partition": itoa(int(partition)),
	}).Add(float64(n))
}

func (m *MQ) IncFetched(group string, partition int32, n int64) {
	if m == nil || n == 0 {
		return
	}
	m.Fetched.With(prometheus.Labels{
		"group": group, "partition": itoa(int(partition)),
	}).Add(float64(n))
}

func (m *MQ) IncRebalance() {
	if m == nil {
		return
	}
	m.Rebalance.Inc()
}

func (m *MQ) SetLag(group, topic string, partition int32, lag float64) {
	if m == nil {
		return
	}
	m.Lag.With(prometheus.Labels{
		"group": group, "topic": topic, "partition": itoa(int(partition)),
	}).Set(lag)
}

func (m *MQ) SetPartitionSize(topic string, partition int32, n float64) {
	if m == nil {
		return
	}
	m.PartitionSize.With(prometheus.Labels{
		"topic": topic, "partition": itoa(int(partition)),
	}).Set(n)
}

func itoa(v int) string {
	return strconv.Itoa(v)
}
