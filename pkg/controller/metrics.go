package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"k8s.io/client-go/util/workqueue"
)

// Per-resource-type workqueue metrics, so a backlog is visible before it
// becomes an OOM. The queue name is the resource type (Pod, Job, ...).
var (
	queueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubewatch_workqueue_depth",
		Help: "Objects with events waiting to be sent, by resource type.",
	}, []string{"name"})
	queueAdds = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubewatch_workqueue_adds_total",
		Help: "Objects added to the send queue, by resource type.",
	}, []string{"name"})
	queueLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kubewatch_workqueue_queue_duration_seconds",
		Help:    "How long an object waits in the send queue before a worker takes it.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
	}, []string{"name"})
	queueWorkDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kubewatch_workqueue_work_duration_seconds",
		Help:    "How long sending one object's pending events takes.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10),
	}, []string{"name"})
	queueUnfinished = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubewatch_workqueue_unfinished_work_seconds",
		Help: "Seconds of work in progress that has not yet finished.",
	}, []string{"name"})
	queueLongestRunning = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubewatch_workqueue_longest_running_processor_seconds",
		Help: "Seconds the longest-running worker has been processing.",
	}, []string{"name"})
	queueRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubewatch_workqueue_retries_total",
		Help: "Retries handled by the send queue, by resource type.",
	}, []string{"name"})
	queueDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kubewatch_events_dropped_total",
		Help: "Events dropped without being sent because the send queue was over its limit, by resource type.",
	}, []string{"resourceType"})
)

type queueMetricsProvider struct{}

func (queueMetricsProvider) NewDepthMetric(name string) workqueue.GaugeMetric {
	return queueDepth.WithLabelValues(name)
}

func (queueMetricsProvider) NewAddsMetric(name string) workqueue.CounterMetric {
	return queueAdds.WithLabelValues(name)
}

func (queueMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramMetric {
	return queueLatency.WithLabelValues(name)
}

func (queueMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	return queueWorkDuration.WithLabelValues(name)
}

func (queueMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return queueUnfinished.WithLabelValues(name)
}

func (queueMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	return queueLongestRunning.WithLabelValues(name)
}

func (queueMetricsProvider) NewRetriesMetric(name string) workqueue.CounterMetric {
	return queueRetries.WithLabelValues(name)
}
