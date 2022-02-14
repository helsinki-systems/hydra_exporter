package collector

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "hydra"

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"hydra_exporter: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"hydra_exporter: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

type HydraCollector struct {
	Collectors map[string]Collector
	logger     log.Logger
}

// Describe implements the prometheus.Collector interface.
func (n HydraCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- scrapeDurationDesc
	ch <- scrapeSuccessDesc
}

// Collect implements the prometheus.Collector interface.
func (n HydraCollector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(n.Collectors))
	for name, c := range n.Collectors {
		go func(name string, c Collector) {
			execute(name, c, ch, n.logger)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(name string, c Collector, ch chan<- prometheus.Metric, logger log.Logger) {
	begin := time.Now()
	err := c.Update(ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		level.Error(logger).Log("msg", "collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		success = 0
	} else {
		level.Debug(logger).Log("msg", "collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}

func NewHydraCollector(logger log.Logger) (*HydraCollector, error) {
	collectors := make(map[string]Collector)

	qrCollector := NewQueueRunnerCollector(logger)
	if qrCollector != nil {
		collectors["queue-runner"] = qrCollector
	}
	notifyCollector := NewNotifyCollector(logger)
	if notifyCollector != nil {
		collectors["notify"] = notifyCollector
	}

	return &HydraCollector{Collectors: collectors, logger: logger}, nil
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}
