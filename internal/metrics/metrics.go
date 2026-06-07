package metrics

import (
	"rs-service/internal/db"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	Namespace = "rs_service"
)

var (
	once     sync.Once
	instance *Metrics
)

type Metrics struct {
	rebuildTotal        prometheus.Counter
	rebuildSuccessTotal prometheus.Counter
	rebuildFailedTotal  prometheus.Counter
	rebuildDuration     prometheus.Histogram
	rebuildAvgDuration  prometheus.Gauge
	nodeStatus          *prometheus.GaugeVec
	filesTotal          prometheus.Gauge
	totalDataSize       prometheus.Gauge
	lazyRebuildTotal    prometheus.Counter
	activeRebuilds      prometheus.Gauge

	rebuildDurations []float64
	mu               sync.Mutex
}

func Get() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			rebuildTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: Namespace,
				Name:      "rebuild_total",
				Help:      "Total number of rebuild operations",
			}),
			rebuildSuccessTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: Namespace,
				Name:      "rebuild_success_total",
				Help:      "Total number of successful rebuild operations",
			}),
			rebuildFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: Namespace,
				Name:      "rebuild_failed_total",
				Help:      "Total number of failed rebuild operations",
			}),
			rebuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
				Namespace: Namespace,
				Name:      "rebuild_duration_seconds",
				Help:      "Duration of rebuild operations in seconds",
				Buckets:   prometheus.DefBuckets,
			}),
			rebuildAvgDuration: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "rebuild_avg_duration_seconds",
				Help:      "Average duration of rebuild operations in seconds",
			}),
			nodeStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "node_status",
				Help:      "Status of storage nodes (1=online, 0=offline)",
			}, []string{"node_id", "node_name"}),
			filesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "files_total",
				Help:      "Total number of stored files",
			}),
			totalDataSize: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "total_data_size_bytes",
				Help:      "Total size of stored data in bytes",
			}),
			lazyRebuildTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: Namespace,
				Name:      "lazy_rebuild_total",
				Help:      "Total number of lazy rebuild operations triggered on access",
			}),
			activeRebuilds: prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: Namespace,
				Name:      "active_rebuilds",
				Help:      "Number of currently active rebuild operations",
			}),
		}

		prometheus.MustRegister(
			instance.rebuildTotal,
			instance.rebuildSuccessTotal,
			instance.rebuildFailedTotal,
			instance.rebuildDuration,
			instance.rebuildAvgDuration,
			instance.nodeStatus,
			instance.filesTotal,
			instance.totalDataSize,
			instance.lazyRebuildTotal,
			instance.activeRebuilds,
		)
	})
	return instance
}

func (m *Metrics) RecordRebuildStart() {
	m.activeRebuilds.Inc()
	m.rebuildTotal.Inc()
}

func (m *Metrics) RecordRebuildComplete(duration time.Duration, success bool, isLazy bool) {
	m.activeRebuilds.Dec()

	durationSec := duration.Seconds()
	m.rebuildDuration.Observe(durationSec)

	if success {
		m.rebuildSuccessTotal.Inc()
	} else {
		m.rebuildFailedTotal.Inc()
	}

	if isLazy {
		m.lazyRebuildTotal.Inc()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.rebuildDurations = append(m.rebuildDurations, durationSec)
	if len(m.rebuildDurations) > 1000 {
		m.rebuildDurations = m.rebuildDurations[1:]
	}

	if len(m.rebuildDurations) > 0 {
		var sum float64
		for _, d := range m.rebuildDurations {
			sum += d
		}
		avg := sum / float64(len(m.rebuildDurations))
		m.rebuildAvgDuration.Set(avg)
	}
}

func (m *Metrics) UpdateNodeStatus(nodeID int, nodeName string, online bool) {
	status := 0.0
	if online {
		status = 1.0
	}
	m.nodeStatus.WithLabelValues(strconv.Itoa(nodeID), nodeName).Set(status)
}

func (m *Metrics) UpdateFileStats(fileCount int, totalSize int64) {
	m.filesTotal.Set(float64(fileCount))
	m.totalDataSize.Set(float64(totalSize))
}

func (m *Metrics) RefreshNodeStatuses() error {
	nodes, err := db.GetNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		online := node.Status == db.NodeStatusOnline
		m.UpdateNodeStatus(node.ID, node.Name, online)
	}
	return nil
}

func (m *Metrics) RefreshFileStats() error {
	files, err := db.ListFiles()
	if err != nil {
		return err
	}
	var totalSize int64
	for _, f := range files {
		totalSize += f.Size
	}
	m.UpdateFileStats(len(files), totalSize)
	return nil
}

func (m *Metrics) RefreshAll() {
	_ = m.RefreshNodeStatuses()
	_ = m.RefreshFileStats()
}
