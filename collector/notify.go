package collector

import (
	"fmt"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/alecthomas/kingpin/v2"
)

var (
	notifyUrl = kingpin.Flag("collector.notify.url", "URL of the notify Prometheus API").String()
)

type notifyCollector struct {
	logger               log.Logger
	url                  string
	eventLoopIterations  *prometheus.Desc
	eventReceived        *prometheus.Desc
	notifyEvent          *prometheus.Desc
	notifyEventError     *prometheus.Desc
	notifyEventRuntime   *prometheus.Desc
	pluginExecutions     *prometheus.Desc
	pluginRuntimes       *prometheus.Desc
	pluginSuccesses      *prometheus.Desc
	pluginErrors         *prometheus.Desc
	pluginRetrySuccesses *prometheus.Desc
	pluginDrops          *prometheus.Desc
	pluginRequeues       *prometheus.Desc
	pluginNotExistent    *prometheus.Desc
	pluginNotInterested  *prometheus.Desc
}

func NewNotifyCollector(logger log.Logger) Collector {
	if *notifyUrl == "" {
		level.Info(logger).Log("msg", "no notify URL configured, not scraping")
		return nil
	}

	return &notifyCollector{
		logger: logger,
		url:    *notifyUrl,
		// Metrics from the event handler
		eventLoopIterations: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "event_loop_iterations"),
			"Number of iterations through the event loop. Incremented at the start of the event loop",
			nil, nil),
		eventReceived: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "events_received"),
			"Timestamp of the last time a new event was received",
			nil, nil),
		notifyEvent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "event_channels_received"),
			"Number of events received on the given channel",
			[]string{"channel"}, nil),
		notifyEventError: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "event_error"),
			"Number of events received that were unprocessable by channel",
			[]string{"channel"}, nil),
		notifyEventRuntime: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "event_runtime"),
			"Number of seconds spent executing events by channel",
			[]string{"channel"}, nil),
		// Metrics from the task dispatcher
		pluginExecutions: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_executions"),
			"Number of times each plugin has been called by channel",
			[]string{"channel", "plugin"}, nil),
		pluginRuntimes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_runtimes"),
			"Number of seconds spent executing each plugin by channel",
			[]string{"channel", "plugin"}, nil),
		pluginSuccesses: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_successes"),
			"Number of successful executions of a plugin on a channel",
			[]string{"channel", "plugin"}, nil),
		pluginErrors: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_errors"),
			"Number of failed executions of a plugin on a channel",
			[]string{"channel", "plugin"}, nil),
		pluginRetrySuccesses: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_retry_successes"),
			"Number of successful executions of retried tasks",
			[]string{"channel", "plugin"}, nil),
		pluginDrops: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_drops"),
			"Number of tasks that have been dropped after too many retries",
			[]string{"channel", "plugin"}, nil),
		pluginRequeues: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_requeues"),
			"Number of tasks that have been requeued after a failure",
			[]string{"channel", "plugin"}, nil),
		pluginNotExistent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_not_existent"),
			"Number of tasks that have not been processed because the plugin does not exist",
			[]string{"channel", "plugin"}, nil),
		pluginNotInterested: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "notify", "plugin_not_interested"),
			"Number of tasks that have not been processed because the plugin was not interested in the event",
			[]string{"channel", "plugin"}, nil),
	}
}

func (c *notifyCollector) Update(ch chan<- prometheus.Metric) error {
	status, err := c.requestNotifyStatus()
	if err != nil {
		return fmt.Errorf("could not get notify status: %w", err)
	}

	// Labelless metrics from the event handler
	if len(status["event_loop_iterations"].GetMetric()) > 0 {
		ch <- prometheus.MustNewConstMetric(c.eventLoopIterations,
			prometheus.CounterValue, *status["event_loop_iterations"].GetMetric()[0].Counter.Value)
	}
	if len(status["event_received"].GetMetric()) > 0 {
		ch <- prometheus.MustNewConstMetric(c.eventReceived,
			prometheus.CounterValue, *status["event_received"].GetMetric()[0].Counter.Value)
	}
	// Labelful metrics from the event handler
	for _, metric := range status["notify_event"].GetMetric() {
		channel := metric.Label[0].Value
		counter := metric.Counter.Value
		ch <- prometheus.MustNewConstMetric(c.notifyEvent,
			prometheus.CounterValue, *counter, *channel)
	}
	for _, metric := range status["notify_event_error"].GetMetric() {
		channel := metric.Label[0].Value
		counter := metric.Counter.Value
		ch <- prometheus.MustNewConstMetric(c.notifyEventError,
			prometheus.CounterValue, *counter, *channel)
	}
	for _, metric := range status["notify_event_runtime"].GetMetric() {
		channel := metric.Label[0].Value
		hist := metric.Histogram
		buckets := make(map[float64]uint64)
		for _, bucket := range hist.GetBucket() {
			buckets[*bucket.UpperBound] = *bucket.CumulativeCount
		}

		ch <- prometheus.MustNewConstHistogram(c.notifyEventRuntime,
			hist.GetSampleCount(), hist.GetSampleSum(), buckets, *channel)
	}
	// Labelful metrics from the task dispatcher
	insertTaskDispatcherCounter := func(desc *prometheus.Desc, srcLabel string) {
		for _, metric := range status[srcLabel].GetMetric() {
			var channel, plugin string
			for _, label := range metric.Label {
				if *label.Name == "channel" {
					channel = *label.Value
				} else if *label.Name == "plugin" {
					plugin = *label.Value
				}
			}
			counter := metric.Counter.Value
			ch <- prometheus.MustNewConstMetric(desc,
				prometheus.CounterValue, *counter, channel, plugin)
		}
	}
	insertTaskDispatcherCounter(c.pluginExecutions, "notify_plugin_executions")
	insertTaskDispatcherCounter(c.pluginSuccesses, "notify_plugin_success")
	insertTaskDispatcherCounter(c.pluginErrors, "notify_plugin_error")
	insertTaskDispatcherCounter(c.pluginRetrySuccesses, "notify_plugin_retry_success")
	insertTaskDispatcherCounter(c.pluginDrops, "notify_plugin_drop")
	insertTaskDispatcherCounter(c.pluginRequeues, "notify_plugin_requeue")
	insertTaskDispatcherCounter(c.pluginNotExistent, "notify_plugin_no_such_plugin")
	insertTaskDispatcherCounter(c.pluginNotInterested, "notify_plugin_not_interested")
	// Histogram from the task dispatcher
	for _, metric := range status["notify_plugin_runtime"].GetMetric() {
		var channel, plugin string
		for _, label := range metric.Label {
			if *label.Name == "channel" {
				channel = *label.Value
			} else if *label.Name == "plugin" {
				plugin = *label.Value
			}
		}
		hist := metric.Histogram
		buckets := make(map[float64]uint64)
		for _, bucket := range hist.GetBucket() {
			buckets[*bucket.UpperBound] = *bucket.CumulativeCount
		}

		ch <- prometheus.MustNewConstHistogram(c.pluginRuntimes,
			hist.GetSampleCount(), hist.GetSampleSum(), buckets, channel, plugin)
	}

	return nil
}

func (c *notifyCollector) requestNotifyStatus() (map[string]*dto.MetricFamily, error) {
	// Do the HTTP request
	resp, err := http.Get(c.url)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}

	// Unmarshal response
	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, err
	}

	return mf, nil
}
