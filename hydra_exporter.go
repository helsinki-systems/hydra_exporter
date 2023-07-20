package main

import (
	"net/http"
	"os"
	"os/user"

	kingpin "github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/kit/log/level"
	"github.com/helsinki-systems/hydra_exporter/collector"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/prometheus/exporter-toolkit/web/kingpinflag"
)

func main() {
	metricsPath := kingpin.Flag(
		"web.telemetry-path",
		"Path under which to expose metrics.",
	).Default("/metrics").String()
	disableExporterMetrics := kingpin.Flag(
		"web.disable-exporter-metrics",
		"Exclude metrics about the exporter itself (process_*, go_*).",
	).Bool()
	toolkitFlags := kingpinflag.AddFlags(kingpin.CommandLine, ":9200")

	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.CommandLine.UsageWriter(os.Stdout)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)

	level.Info(logger).Log("msg", "Starting hydra_exporter")
	if user, err := user.Current(); err == nil && user.Uid == "0" {
		level.Warn(logger).Log("msg", "Hydra Exporter is running as root user. This exporter is designed to run as unpriviledged user, root is not required.")
	}

	r := prometheus.NewRegistry()
	hc, err := collector.NewHydraCollector(logger)
	if err != nil {
		level.Error(logger).Log("err: couldn't create collector", err)
		os.Exit(1)
	}
	if !*disableExporterMetrics {
		r.MustRegister(
			promcollectors.NewProcessCollector(promcollectors.ProcessCollectorOpts{}),
			promcollectors.NewGoCollector(),
		)
	}
	r.MustRegister(hc)
	handler := promhttp.HandlerFor(r, promhttp.HandlerOpts{})

	http.Handle("/metrics", handler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Hydra Exporter</title></head>
			<body>
			<h1>Hydra Exporter</h1>
			<p><a href="` + *metricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	server := &http.Server{}
	if err := web.ListenAndServe(server, toolkitFlags, logger); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
}
