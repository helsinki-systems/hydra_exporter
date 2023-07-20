package collector

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/alecthomas/kingpin/v2"
)

// Data is from Hydra 4acaf9c8b05dbdac88c7fbf153a11282fc4096f1
type queueRunnerResponse struct {
	Status                    string                                    `json:"status"` // not exported
	Time                      float64                                   `json:"time" promtype:"counter" prom:"time,The queue runner's current time"`
	Uptime                    float64                                   `json:"uptime" promtype:"counter" prom:"uptime,The queue runner's current uptime"`
	Pid                       int                                       `json:"pid"` // not exported
	NrQueuedBuilds            float64                                   `json:"nrQueuedBuilds" promtype:"gauge" prom:"builds_queued,Current build queue size"`
	NrUnfinishedSteps         float64                                   `json:"nrUnfinishedSteps" promtype:"gauge" prom:"steps_queued,Current number of steps for the build queue"`
	NrRunnableSteps           float64                                   `json:"nrRunnableSteps" promtype:"gauge" prom:"steps_runnable,Current number of steps which can run immediately"`
	NrActiveSteps             float64                                   `json:"nrActiveSteps" promtype:"gauge" prom:"steps_active,Current number of steps which are currently active"`
	NrStepsBuilding           float64                                   `json:"nrStepsBuilding" promtype:"gauge" prom:"steps_building,Current number of steps which are currently building"`
	NrStepsCopyingTo          float64                                   `json:"nrStepsCopyingTo" promtype:"gauge" prom:"steps_copying_to,Current number of steps which are having build inputs copied to a builder"`
	NrStepsCopyingFrom        float64                                   `json:"nrStepsCopyingFrom" promtype:"gauge" prom:"steps_copying_from,Current number of steps which are having build results copied from a builder"`
	NrStepsWaiting            float64                                   `json:"nrStepsWaiting" promtype:"gauge" prom:"steps_waiting,Current number of steps which are waiting"`
	NrUnsupportedSteps        float64                                   `json:"nrUnsupportedSteps" promtype:"gauge" prom:"steps_unsupported,Number of steps that are not supported by this queue runner"`
	BytesSent                 float64                                   `json:"bytesSent" promtype:"counter" prom:"build_inputs_sent_bytes,Total count of bytes sent as build inputs"`
	BytesReceived             float64                                   `json:"bytesReceived" promtype:"counter" prom:"build_outputs_received_bytes,Total count of bytes received asbuild outputs"`
	NrBuildsRead              float64                                   `json:"nrBuildsRead" promtype:"counter" prom:"builds_read,Total count of builds whose outputs have been read"`
	BuildReadTimeMs           float64                                   `json:"buildReadTimeMs" promtype:"counter" prom:"builds_read_ms,Total number of milliseconds spent reading build outputs"`
	BuildReadTimeAvgMs        float64                                   `json:"buildReadTimeAvgMs"` // not exported
	NrBuildsDone              float64                                   `json:"nrBuildsDone" promtype:"counter" prom:"builds_done,Total count of builds performed"`
	NrStepsStarted            float64                                   `json:"nrStepsStarted" promtype:"counter" prom:"steps_started,Total count of steps started"`
	NrStepsDone               float64                                   `json:"nrStepsDone" promtype:"counter" prom:"steps_done,Total count of steps completed"`
	NrRetries                 float64                                   `json:"nrRetries" promtype:"counter" prom:"retries,Total number of retries"`
	MaxNrRetries              float64                                   `json:"maxNrRetries" promtype:"counter" prom:"max_retries,Maximum number of retries for any single job"`
	TotalStepTime             float64                                   `json:"totalStepTime" promtype:"counter" prom:"step_time,Total time spent executing steps"`
	TotalStepBuildTime        float64                                   `json:"totalStepBuildTime" promtype:"counter" prom:"step_build_time,Total time spent in the building phase of a build step"`
	AvgStepTime               float64                                   `json:"avgStepTime"`      // not exported
	AvgStepBuildTime          float64                                   `json:"avgStepBuildTime"` // not exported
	NrQueueWakeups            float64                                   `json:"nrQueueWakeups" promtype:"counter" prom:"queue_wakeups,Count of the times the queue runner has been notified of queue changes"`
	NrDispatcherWakeups       float64                                   `json:"nrDispatcherWakeups" promtype:"counter" prom:"dispatcher_wakeup,Count of the times the queue runner work dispatcher woke up due to new runnable builds and completed builds"`
	DispatchTimeMs            float64                                   `json:"dispatchTimeMs" promtype:"counter" prom:"dispatch_execution_ms,Number of milliseconds the dispatcher has spent working"`
	DispatchTimeAvgMs         float64                                   `json:"dispatchTimeAvgMs"` // not exported
	NrDbConnections           float64                                   `json:"nrDbConnections" promtype:"gauge" prom:"db_connections,Number of connections to the database"`
	NrActiveDbUpdates         float64                                   `json:"nrActiveDbUpdates" promtype:"gauge" prom:"db_updates,Number of in-progress database updates"`
	MemoryTokensInUse         float64                                   `json:"memoryTokensInUse" promtype:"gauge" prom:"memory_tokens,Number of memory tokens in use"`
	NrNotificationsDone       float64                                   `json:"nrNotificationsDone" promtype:"counter" prom:"notifications_total,Total number of notifications sent"`
	NrNotificationsFailed     float64                                   `json:"nrNotificationsFailed" promtype:"counter" prom:"notifications_failed,Number of notifications failed"`
	NrNotificationsInProgress float64                                   `json:"nrNotificationsInProgress" promtype:"counter" prom:"notifications_in_progress,Number of notifications in_progress"`
	NrNotificationsPending    float64                                   `json:"nrNotificationsPending" promtype:"counter" prom:"notifications_pending,Number of notifications pending"`
	NrNotificationTimeMs      float64                                   `json:"nrNotificationTimeMs" promtype:"counter" prom:"notifications_seconds,Time spent delivering notifications"`
	NrNotificationTimeAvgMs   float64                                   `json:"nrNotificationTimeAvgMs"` // not exported
	Machines                  map[string]queueRunnerMachineReponse      `json:"machines" promlabel:"host"`
	Jobsets                   map[string]queueRunnerJobsetsReponse      `json:"jobsets" promlabel:"name"`
	MachineTypes              map[string]queueRunnerMachineTypesReponse `json:"machineTypes" promlabel:"machineType"`
	Store                     queueRunnerStoreReponse                   `json:"store"`
}

type queueRunnerMachineReponse struct {
	Enabled             bool     `json:"enabled" promtype:"gauge" prom:"machine_enabled,Whether the machine is enabled (1) or not (0)"`
	SystemTypes         []string `json:"systemTypes"`       // not exported
	SupportedFeatures   []string `json:"supportedFeatures"` // not exported
	MandatoryFeatures   []string `json:"mandatoryFeatures"` // not exported
	CurrentJobs         float64  `json:"currentJobs" promtype:"gauge" prom:"machine_current_jobs,Number of current jobs"`
	IdleSince           float64  `json:"idleSince" promtype:"gauge" prom:"machine_idle_since,When the current idle period started"`
	NrStepsDone         float64  `json:"nrStepsDone" promtype:"counter" prom:"machine_steps_done,Count of steps this machine has completed"`
	TotalStepTime       float64  `json:"totalStepTime" promtype:"counter" prom:"machine_step_time,Time this machine has spent running steps"`
	TotalStepBuildTime  float64  `json:"totalStepBuildTime" promtype:"counter" prom:"machine_step_build_time,Time this machine has spent in the building phase of a step"`
	AvgStepTime         float64  `json:"avgStepTime"`      // not exported
	AvgStepBuildTime    float64  `json:"avgStepBuildTime"` // not exported
	DisabledUntil       float64  `json:"disabledUntil" promtype:"gauge" prom:"machine_disabled_until,When the machine will be used again"`
	LastFailure         float64  `json:"lastFailure" promtype:"gauge" prom:"machine_last_failure,Timestamp of the last failure"`
	ConsecutiveFailures float64  `json:"consecutiveFailures" promtype:"gauge" prom:"machine_consecutive_failures,Number of consecutive failed builds"`
}

type queueRunnerJobsetsReponse struct {
	ShareUsed float64 `json:"shareUsed" promtype:"counter" prom:"jobset_shares_used_total,Total shares the jobset has consumed"`
	Seconds   float64 `json:"seconds" promtype:"counter" prom:"jobset_seconds_total,Total number of seconds the jobset has been building"`
}

type queueRunnerMachineTypesReponse struct {
	Runnable   float64 `json:"runnable" promtype:"gauge" prom:"machine_type_runnable,Number of currently runnable builds"`
	Running    float64 `json:"running" promtype:"gauge" prom:"machine_type_running,Number of currently running builds"`
	WaitTime   float64 `json:"waitTime" promtype:"counter" prom:"machine_type_wait_time_total,Number of seconds spent waiting"`
	LastActive float64 `json:"lastActive" promtype:"gauge" prom:"machine_type_last_active_total,Last time this machine type was active"`
}

type queueRunnerStoreReponse struct {
	NarInfoRead               float64               `json:"narInfoRead" promtype:"counter" prom:"store_nar_info_read,Number of NarInfo files read from the binary cache"`
	NarInfoReadAverted        float64               `json:"narInfoReadAverted" promtype:"counter" prom:"store_nar_info_read_averted,Number of NarInfo files reads which were avoided"`
	NarInfoMissing            float64               `json:"narInfoMissing" promtype:"counter" prom:"store_nar_info_missing,Number of NarInfo files read attempts which identified a missing narinfo file"`
	NarInfoWrite              float64               `json:"narInfoWrite" promtype:"counter" prom:"store_nar_info_write,Number of NarInfo files written to the binary cache"`
	NarInfoCacheSize          float64               `json:"narInfoCacheSize" promtype:"counter" prom:"store_nar_info_cache_size,Size of the in-memory store path information cache"`
	NarRead                   float64               `json:"narRead" promtype:"counter" prom:"store_nar_read,Number of NAR files read from the binary cache"`
	NarReadBytes              float64               `json:"narReadBytes" promtype:"counter" prom:"store_nar_read_bytes,Number of NAR file bytes read after decompression from the binary cache"`
	NarReadCompressedBytes    float64               `json:"narReadCompressedBytes" promtype:"counter" prom:"store_nar_read_compressed_bytes,Number of NAR file bytes read before decompression from the binary cache"`
	NarWrite                  float64               `json:"narWrite" promtype:"counter" prom:"store_nar_write,Number of NAR files written to the binary cache"`
	NarWriteAverted           float64               `json:"narWriteAverted" promtype:"counter" prom:"store_nar_write_averted,Number of NAR files writes skipped due to the NAR already being in the binary cache"`
	NarWriteBytes             float64               `json:"narWriteBytes" promtype:"counter" prom:"store_nar_write_bytes,Number of NAR file bytes written after decompression to the binary cache"`
	NarWriteCompressedBytes   float64               `json:"narWriteCompressedBytes" promtype:"counter" prom:"store_nar_write_compressed_bytes,Number of NAR file bytes written before decompression to the binary cache"`
	NarWriteCompressionTimeMs float64               `json:"narWriteCompressionTimeMs" promtype:"counter" prom:"store_nar_write_compression_ms,Number of seconds spent compressing data when writing NARs to the binary cache"`
	NarCompressionSavings     float64               `json:"narCompressionSavings" promtype:"gauge" prom:"store_nar_compression_savings,Amount of savings due to compression"`
	NarCompressionSpeed       float64               `json:"narCompressionSpeed" promtype:"gauge" prom:"store_nar_compression_speed,NAR compression speed in MiB/s"`
	S3                        queueRunnerS3Response `json:"s3"`
}

type queueRunnerS3Response struct {
	Put              float64 `json:"put" promtype:"counter" prom:"store_s3_put,Number of PUTs to S3"`
	PutBytes         float64 `json:"putBytes" promtype:"counter" prom:"store_s3_put_bytes,Number of bytes written to S3"`
	PutTimeMs        float64 `json:"putTimeMs" promtype:"counter" prom:"store_s3_put_ms,Number of seconds spent writing to S3"`
	PutSpeed         float64 `json:"putSpeed" promtype:"gauge" prom:"store_s3_put_speed,PUT speed in MiB/s"`
	Get              float64 `json:"get" promtype:"counter" prom:"store_s3_get,Number of GETs to S3"`
	GetBytes         float64 `json:"getBytes" promtype:"counter" prom:"store_s3_get_bytes,Number of bytes read from S3"`
	GetTimeMs        float64 `json:"getTimeMs" promtype:"counter" prom:"store_s3_get_ms,Number of seconds spent reading from S3"`
	GetSpeed         float64 `json:"getSpeed" promtype:"gauge" prom:"store_s3_get_speed,GET speed in MiB/s"`
	Head             float64 `json:"head" promtype:"counter" prom:"store_s3_head,Number of HEADs to S3"`
	CostDollarApprox float64 `json:"costDollarApprox" promtype:"gauge" prom:"store_s3_cost_approximate_dollars,Estimated cost of the S3 bucket activity"`
}

type queueRunnerCollector struct {
	logger      log.Logger
	url         string
	descriptors map[string]*prometheus.Desc
}

var (
	queueRunnerUrl = kingpin.Flag("collector.queue-runner.url", "URL of the queue runner API").String()
)

func NewQueueRunnerCollector(logger log.Logger) Collector {
	descriptors := make(map[string]*prometheus.Desc)

	if *queueRunnerUrl == "" {
		level.Info(logger).Log("msg", "no queue runner URL configured, not scraping")
		return nil
	}

	// Insert descriptors
	var insertDescriptors func(reflect.Value, string, string)
	insertDescriptors = func(val reflect.Value, superField string, dynamicLabel string) {
		for i := 0; i < val.NumField(); i++ {
			field := val.Type().Field(i)
			fieldName := string(field.Name)
			// Recurse into structs if needed
			if field.Type.Kind() == reflect.Struct {
				insertDescriptors(reflect.New(field.Type).Elem(), "", "")
				continue
			}
			// Recurse into maps of structs if needed
			if field.Type.Kind() == reflect.Map {
				label, _ := parseTag(string(field.Tag.Get("promlabel")))
				insertDescriptors(reflect.New(field.Type.Elem()).Elem(), fieldName, label)
				continue
			}
			// Get the internal field name, the exported name, and the help
			exportedName, help := parseTag(string(field.Tag.Get("prom")))
			if exportedName == "" {
				continue // we don't export some values
			}
			// Generate the variable labels
			var variableLabels []string
			if dynamicLabel == "" {
				variableLabels = nil
			} else {
				variableLabels = []string{dynamicLabel}
			}
			// Insert into the descriptors
			descriptors[superField+"/"+fieldName] = prometheus.NewDesc(prometheus.BuildFQName(namespace, "queue_runner", exportedName), help, variableLabels, nil)
		}
	}
	// Start the recursion
	insertDescriptors(reflect.ValueOf(&queueRunnerResponse{}).Elem(), "", "")

	return &queueRunnerCollector{
		logger:      logger,
		url:         *queueRunnerUrl,
		descriptors: descriptors,
	}
}

func (c *queueRunnerCollector) Update(ch chan<- prometheus.Metric) error {
	status, err := c.requestQRStatus()
	if err != nil {
		return fmt.Errorf("could not get queue runner status: %w", err)
	}

	// Expose metrics
	var exposeMetrics func(interface{}, string, string)
	exposeMetrics = func(val interface{}, superField string, dynamicLabel string) {
		typ := reflect.ValueOf(val).Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fieldName := string(field.Name)

			// Recurse into structs if needed
			if field.Type.Kind() == reflect.Struct {
				lal := reflect.ValueOf(val).Field(i).Interface()
				exposeMetrics(lal, "", "")
				continue
			}

			// Recurse into maps of structs if needed
			if field.Type.Kind() == reflect.Map {
				m := reflect.ValueOf(val).Field(i)
				for _, key := range m.MapKeys() {
					if superField == "" {
						exposeMetrics(m.MapIndex(key).Interface(), fieldName, key.String())
					} else {
						exposeMetrics(m.MapIndex(key).Interface(), superField+"/"+fieldName, key.String())
					}
				}
				continue
			}

			promType, _ := parseTag(string(field.Tag.Get("promtype")))
			if promType == "" {
				continue // we don't export some values
			}
			var t prometheus.ValueType
			if promType == "counter" {
				t = prometheus.CounterValue
			} else if promType == "gauge" {
				t = prometheus.GaugeValue
			} else {
				panic("Unknown Prometheus type " + promType)
			}
			// Get the value and convert it
			value := reflect.ValueOf(val).Field(i)
			var ret float64
			switch value.Kind() {
			case reflect.Float64:
				ret = value.Float()
			case reflect.Bool:
				if value.Bool() {
					ret = 1.0
				} else {
					ret = 0.0
				}
			default:
				panic("Unknown variable type " + value.String())
			}
			// Generate the variable labels
			var variableLabels []string
			if dynamicLabel == "" {
				variableLabels = nil
			} else {
				variableLabels = []string{dynamicLabel}
			}
			// Expose the metric
			ch <- prometheus.MustNewConstMetric(c.descriptors[superField+"/"+fieldName], t, ret, variableLabels...)
		}
	}
	exposeMetrics(status, "", "")

	return nil
}

func (c *queueRunnerCollector) requestQRStatus() (queueRunnerResponse, error) {
	// Do the HTTP request
	req, err := http.NewRequest("GET", c.url, nil)
	if err != nil {
		return queueRunnerResponse{}, fmt.Errorf("http request not created: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return queueRunnerResponse{}, fmt.Errorf("http request failed: %w", err)
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return queueRunnerResponse{}, fmt.Errorf("unable to read body: %w", err)
	}

	// Unmarshal response
	var ret queueRunnerResponse
	err = json.Unmarshal(bodyBytes, &ret)
	if err != nil {
		return queueRunnerResponse{}, fmt.Errorf("unable to decode body: %w", err)
	}

	// Unmarshal again into interface
	var rawRet map[string]interface{}
	err = json.Unmarshal(bodyBytes, &rawRet)
	if err != nil {
		return queueRunnerResponse{}, fmt.Errorf("unable to decode body again: %w", err)
	}

	var extra []string
	// TODO solve with reflection and recursion
	// List toplevel keys
	knownKeys := make(map[string]bool)
	val := reflect.ValueOf(&ret).Elem()
	for i := 0; i < val.NumField(); i++ {
		key, _ := parseTag(string(val.Type().Field(i).Tag.Get("json")))
		knownKeys[key] = true
	}
	// Check for extra keys in toplevel
	for k := range rawRet {
		if _, ok := knownKeys[k]; !ok {
			extra = append(extra, k)
		}
	}
	// List machines keys
	knownKeys = make(map[string]bool)
	val = reflect.ValueOf(&queueRunnerMachineReponse{}).Elem()
	for i := 0; i < val.NumField(); i++ {
		key, _ := parseTag(string(val.Type().Field(i).Tag.Get("json")))
		knownKeys[key] = true
	}
	// Check for extra keys in machines
	for machineName, machine := range rawRet["machines"].(map[string]interface{}) {
		for k := range machine.(map[string]interface{}) {
			if _, ok := knownKeys[k]; !ok {
				extra = append(extra, "machines."+machineName+"."+k)
			}
		}
	}
	// List jobsets keys
	knownKeys = make(map[string]bool)
	val = reflect.ValueOf(&queueRunnerJobsetsReponse{}).Elem()
	for i := 0; i < val.NumField(); i++ {
		key, _ := parseTag(string(val.Type().Field(i).Tag.Get("json")))
		knownKeys[key] = true
	}
	// Check for extra keys in machines
	for jobsetName, jobset := range rawRet["jobsets"].(map[string]interface{}) {
		for k := range jobset.(map[string]interface{}) {
			if _, ok := knownKeys[k]; !ok {
				extra = append(extra, "jobsets."+jobsetName+"."+k)
			}
		}
	}
	// List machine types keys
	knownKeys = make(map[string]bool)
	val = reflect.ValueOf(&queueRunnerMachineTypesReponse{}).Elem()
	for i := 0; i < val.NumField(); i++ {
		key, _ := parseTag(string(val.Type().Field(i).Tag.Get("json")))
		knownKeys[key] = true
	}
	// Check for extra keys in machine types
	for mtName, mt := range rawRet["machineTypes"].(map[string]interface{}) {
		for k := range mt.(map[string]interface{}) {
			if _, ok := knownKeys[k]; !ok {
				extra = append(extra, "jobsets."+mtName+"."+k)
			}
		}
	}
	// List store keys
	knownKeys = make(map[string]bool)
	val = reflect.ValueOf(&queueRunnerStoreReponse{}).Elem()
	for i := 0; i < val.NumField(); i++ {
		key, _ := parseTag(string(val.Type().Field(i).Tag.Get("json")))
		knownKeys[key] = true
	}
	// Check for extra keys in machine types
	for k := range rawRet["store"].(map[string]interface{}) {
		if _, ok := knownKeys[k]; !ok {
			extra = append(extra, "store."+k)
		}
	}

	if len(extra) != 0 {
		level.Warn(c.logger).Log("msg", "extra keys returned by queue runner", "keys", strings.Join(extra, ","))
	}

	return ret, nil
}

func parseTag(tag string) (string, string) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], string(tag[idx+1:])
	}
	return tag, string("")
}
