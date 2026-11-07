/*
Copyright 2018 Bitnami

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cloudevent

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/bitnami-labs/kubewatch/pkg/collector"
	"github.com/bitnami-labs/kubewatch/pkg/event"
	"k8s.io/apimachinery/pkg/runtime"
)

var cloudEventErrMsg = `
%s

You need to set Cloudevents webhook url
using "--url/-u" or using environment variables:

export KW_CLOUDEVENT_URL=webhook_url

Command line flags will override environment variables

`

// CloudEvent handler implements handler.Handler interface,
// Notify event to CloudEvent endpoint
type CloudEvent struct {
	Url        string
	StartTime  uint64
	Counter    uint64
	httpClient *http.Client
}

type CloudEventMessage struct {
	SpecVersion     string                `json:"specversion"`
	Type            string                `json:"type"`
	Source          string                `json:"source"`
	Subject         string                `json:"subject"`
	ID              string                `json:"id"`
	Time            time.Time             `json:"time"`
	DataContentType string                `json:"datacontenttype"`
	Data            CloudEventMessageData `json:"data"`
}

// EventMeta containes the meta data about the event occurred
type CloudEventMessageData struct {
	Operation   string         `json:"operation"`
	Kind        string         `json:"kind"`
	ClusterUid  string         `json:"clusterUid"`
	Description string         `json:"description"`
	ApiVersion  string         `json:"apiVersion"`
	Obj         runtime.Object `json:"obj"`
	OldObj      runtime.Object `json:"oldObj"`
}

// SyncWebhookMessage reused from webhook package for consistency
type SyncWebhookMessage struct {
	Type            string                            `json:"type"`
	Timestamp       time.Time                         `json:"timestamp"`
	ClusterSnapshot collector.ClusterWorkloadSnapshot `json:"clusterSnapshot"`
	Metadata        collector.WorkloadSyncMetadata    `json:"metadata"`
}

func (m *CloudEvent) Init(c *config.Config) error {
	m.Url = c.Handler.CloudEvent.Url
	m.StartTime = uint64(time.Now().Unix())
	m.Counter = 0

	if m.Url == "" {
		m.Url = os.Getenv("KW_CLOUDEVENT_URL")
	}

	if m.Url == "" {
		return fmt.Errorf(cloudEventErrMsg, "Missing cloudevent url")
	}

	// Create dedicated HTTP client for this handler
	// Use sync timeout if available, otherwise default to 30s
	timeout := 30 * time.Second
	if c.Sync.TimeoutSeconds > 0 {
		timeout = time.Duration(c.Sync.TimeoutSeconds) * time.Second
	}

	// Several workers per resource type send through this client at once.
	// The default transport keeps only two idle connections per host, so
	// the rest would be re-dialled for every event.
	var transport http.RoundTripper = http.DefaultTransport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t = t.Clone()
		t.MaxIdleConnsPerHost = 64
		transport = t
	}
	m.httpClient = &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	return nil
}

// Handle is called concurrently by every resource type's workers.
func (m *CloudEvent) Handle(e event.Event) {
	message := m.prepareMessage(e, atomic.AddUint64(&m.Counter, 1))

	err := m.postMessage(message)
	if err != nil {
		logrus.Printf("%s\n", err)
		return
	}

	logrus.Debugf("Message successfully sent to %s at %s ", m.Url, time.Now())
}

func (m *CloudEvent) prepareMessage(e event.Event, seq uint64) *CloudEventMessage {
	return &CloudEventMessage{
		SpecVersion:     "1.0",
		Type:            "KUBERNETES_TOPOLOGY_CHANGE",
		Source:          "https://github.com/nudgebee/kubewatch",
		ID:              fmt.Sprintf("%v-%v", m.StartTime, seq),
		Time:            time.Now(), // TODO: verify that time format is correct - note that this is the time of sending not time of event
		DataContentType: "application/json",
		Data: CloudEventMessageData{
			Operation:   m.formatReason(e),
			Kind:        e.Kind,
			ApiVersion:  e.ApiVersion,
			ClusterUid:  "",
			Description: e.Message(),
			Obj:         e.Obj,
			OldObj:      e.OldObj,
		},
	}
}

func (m *CloudEvent) formatReason(e event.Event) string {
	switch e.Reason {
	case "Created":
		return "create"
	case "Updated":
		return "update"
	case "Deleted":
		return "delete"
	default:
		return "unknown"
	}
}

func (m *CloudEvent) postMessage(webhookMessage *CloudEventMessage) error {
	message, err := json.Marshal(webhookMessage)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", m.Url, bytes.NewBuffer(message))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Read the body to the end so the connection goes back to the pool.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 400 {
		return fmt.Errorf("cloudevent endpoint %s returned HTTP %d", m.Url, resp.StatusCode)
	}

	return nil
}

// HandleWorkloadSnapshot handles workload snapshot data.
// Snapshots are sent un-enveloped (not wrapped in CloudEvent format) so the
// receiver consumes the same payload shape as the webhook handler emits.
func (m *CloudEvent) HandleWorkloadSnapshot(snapshot *collector.ClusterWorkloadSnapshot, metadata collector.WorkloadSyncMetadata) error {
	atomic.AddUint64(&m.Counter, 1)

	message := &SyncWebhookMessage{
		Type:            "cluster_snapshot",
		Timestamp:       time.Now(),
		ClusterSnapshot: *snapshot,
		Metadata:        metadata,
	}

	return m.postSyncMessage(message)
}

// postSyncMessage sends a sync message (same format as webhook)
func (m *CloudEvent) postSyncMessage(message *SyncWebhookMessage) error {
	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal sync message: %w", err)
	}

	req, err := http.NewRequest("POST", m.Url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send sync message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("endpoint returned error status: %d", resp.StatusCode)
	}

	logrus.Printf("Workload snapshot successfully sent to %s at %s", m.Url, time.Now())
	return nil
}
