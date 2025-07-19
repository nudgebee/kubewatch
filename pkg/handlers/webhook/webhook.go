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

package webhook

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"

	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"time"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/bitnami-labs/kubewatch/pkg/collector"
	"github.com/bitnami-labs/kubewatch/pkg/event"
)

var webhookErrMsg = `
%s

You need to set Webhook url, and Webhook cert if you use self signed certificates,
using "--url/-u" and "--cert", or using environment variables:

export KW_WEBHOOK_URL=webhook_url
export KW_WEBHOOK_CERT=/path/of/cert

Command line flags will override environment variables

`

// Webhook handler implements handler.Handler interface,
// Notify event to Webhook channel
type Webhook struct {
	Url         string
	Compression bool
	httpClient  *http.Client
}

// WebhookMessage for messages
type WebhookMessage struct {
	EventMeta EventMeta `json:"eventmeta"`
	Text      string    `json:"text"`
	Time      time.Time `json:"time"`
}

// SyncWebhookMessage for sync operations - keeps same data but adds type field
type SyncWebhookMessage struct {
	Type            string                            `json:"type"`
	Timestamp       time.Time                         `json:"timestamp"`
	ClusterSnapshot collector.ClusterWorkloadSnapshot `json:"clusterSnapshot"`
	Metadata        collector.WorkloadSyncMetadata    `json:"metadata"`
}

// EventMeta containes the meta data about the event occurred
type EventMeta struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Reason    string `json:"reason"`
}

// Init prepares Webhook configuration
func (m *Webhook) Init(c *config.Config) error {
	url := c.Handler.Webhook.Url
	cert := c.Handler.Webhook.Cert
	tlsSkip := c.Handler.Webhook.TlsSkip
	compression := c.Handler.Webhook.Compression

	if url == "" {
		url = os.Getenv("KW_WEBHOOK_URL")
	}
	if cert == "" {
		cert = os.Getenv("KW_WEBHOOK_CERT")
	}

	m.Url = url
	m.Compression = compression

	// Create TLS config
	var tlsConfig *tls.Config
	if tlsSkip {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	} else {
		if cert == "" {
			logrus.Printf("No webhook cert is given")
			tlsConfig = &tls.Config{}
		} else {
			caCert, err := os.ReadFile(cert)
			if err != nil {
				logrus.Printf("%s\n", err)
				return err
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig = &tls.Config{RootCAs: caCertPool}
		}
	}

	// Determine timeout - use sync timeout if available, otherwise default to 30s
	timeout := 30 * time.Second
	if c.Sync.TimeoutSeconds > 0 {
		timeout = time.Duration(c.Sync.TimeoutSeconds) * time.Second
	}

	// Create dedicated HTTP client for this webhook
	m.httpClient = &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return checkMissingWebhookVars(m)
}

// Handle handles an event.
func (m *Webhook) Handle(e event.Event) {
	webhookMessage := prepareWebhookMessage(e, m)

	message, err := json.Marshal(webhookMessage)
	if err != nil {
		logrus.Printf("Failed to marshal webhook message: %s\n", err)
		return
	}

	req, err := http.NewRequest("POST", m.Url, bytes.NewBuffer(message))
	if err != nil {
		logrus.Printf("Failed to create request: %s\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = m.httpClient.Do(req)
	if err != nil {
		logrus.Printf("Failed to send webhook message: %s\n", err)
		return
	}

	logrus.Printf("Message successfully sent to %s at %s ", m.Url, time.Now())
}

// HandleWorkloadSnapshot handles workload snapshot data
func (m *Webhook) HandleWorkloadSnapshot(snapshot *collector.ClusterWorkloadSnapshot, metadata collector.WorkloadSyncMetadata) error {
	message := &SyncWebhookMessage{
		Type:            "cluster_snapshot",
		Timestamp:       time.Now(),
		ClusterSnapshot: *snapshot,
		Metadata:        metadata,
	}

	if m.Compression {
		return m.sendCompressedWorkloadSnapshot(message)
	}
	return m.sendSyncMessage(message)
}

// sendCompressedWorkloadSnapshot sends compressed workload snapshot
func (m *Webhook) sendCompressedWorkloadSnapshot(message *SyncWebhookMessage) error {
	// Serialize to JSON
	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal workload snapshot: %w", err)
	}

	// Compress with gzip
	var compressed bytes.Buffer
	gzWriter := gzip.NewWriter(&compressed)
	if _, err := gzWriter.Write(jsonData); err != nil {
		gzWriter.Close()
		return fmt.Errorf("failed to compress workload snapshot: %w", err)
	}
	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	// Create HTTP request with proper headers
	req, err := http.NewRequest("POST", m.Url, &compressed)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Kubewatch-Event-Type", "cluster_snapshot")
	req.Header.Set("X-Kubewatch-Compressed", "true")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send compressed workload snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned error status: %d", resp.StatusCode)
	}

	logrus.Printf("Workload snapshot successfully sent to %s (compressed: %d bytes) at %s",
		m.Url, compressed.Len(), time.Now())
	return nil
}

// sendSyncMessage sends uncompressed sync message
func (m *Webhook) sendSyncMessage(message *SyncWebhookMessage) error {
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
		return fmt.Errorf("webhook returned error status: %d", resp.StatusCode)
	}

	logrus.Printf("Sync message successfully sent to %s at %s", m.Url, time.Now())
	return nil
}

func checkMissingWebhookVars(s *Webhook) error {
	if s.Url == "" {
		return fmt.Errorf(webhookErrMsg, "Missing Webhook url")
	}

	return nil
}

func prepareWebhookMessage(e event.Event, m *Webhook) *WebhookMessage {
	return &WebhookMessage{
		EventMeta: EventMeta{
			Kind:      e.Kind,
			Name:      e.Name,
			Namespace: e.Namespace,
			Reason:    e.Reason,
		},
		Text: e.Message(),
		Time: time.Now(),
	}
}

