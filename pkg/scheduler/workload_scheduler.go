/*
Copyright 2024 Robusta Dev

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

package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/bitnami-labs/kubewatch/pkg/collector"
	"github.com/bitnami-labs/kubewatch/pkg/handlers"
	"github.com/sirupsen/logrus"
	
	"k8s.io/client-go/kubernetes"
)

// WorkloadScheduler manages periodic workload snapshot collection and delivery
type WorkloadScheduler struct {
	client    kubernetes.Interface
	config    *config.Config
	collector *collector.WorkloadCollector
	handler   handlers.Handler
	ticker    *time.Ticker
	stopCh    chan struct{}
	logger    *logrus.Entry
}

// NewWorkloadScheduler creates a new workload scheduler
func NewWorkloadScheduler(client kubernetes.Interface, config *config.Config, handler handlers.Handler) *WorkloadScheduler {
	return &WorkloadScheduler{
		client:    client,
		config:    config,
		collector: collector.NewWorkloadCollector(client, &config.Sync),
		handler:   handler,
		stopCh:    make(chan struct{}),
		logger:    logrus.WithField("component", "workload-scheduler"),
	}
}

// Start starts the workload scheduler
func (ws *WorkloadScheduler) Start() error {
	if !ws.config.Sync.Enabled || !ws.config.Sync.WorkloadSnapshot.Enabled {
		ws.logger.Info("Workload sync is disabled")
		return nil
	}

	// Parse interval
	interval, err := time.ParseDuration(ws.config.Sync.Interval)
	if err != nil {
		return fmt.Errorf("invalid sync interval %s: %w", ws.config.Sync.Interval, err)
	}

	ws.logger.WithField("interval", interval).Info("Starting workload scheduler")
	
	// Set up ticker for periodic sync
	ws.ticker = time.NewTicker(interval)
	
	// Run initial sync
	go ws.runSync()
	
	// Start periodic sync
	go ws.runPeriodicSync()
	
	return nil
}

// Stop stops the workload scheduler
func (ws *WorkloadScheduler) Stop() {
	ws.logger.Info("Stopping workload scheduler")
	close(ws.stopCh)
	if ws.ticker != nil {
		ws.ticker.Stop()
	}
}

// runPeriodicSync runs the periodic sync loop
func (ws *WorkloadScheduler) runPeriodicSync() {
	for {
		select {
		case <-ws.ticker.C:
			ws.runSync()
		case <-ws.stopCh:
			ws.logger.Info("Workload scheduler stopped")
			return
		}
	}
}

// runSync executes a single sync operation with retry logic
func (ws *WorkloadScheduler) runSync() {
	ws.logger.Info("Starting workload sync")
	startTime := time.Now()
	
	err := ws.runSyncWithRetry()
	if err != nil {
		ws.logger.WithError(err).Error("Failed to complete workload sync after all retries")
		return
	}
	
	duration := time.Since(startTime)
	ws.logger.WithField("duration", duration).Info("Workload sync completed successfully")
}

// runSyncWithRetry executes sync operation with configurable retry logic
func (ws *WorkloadScheduler) runSyncWithRetry() error {
	var lastErr error
	
	// Parse retry intervals
	initialInterval, err := time.ParseDuration(ws.config.Sync.Retry.InitialInterval)
	if err != nil {
		ws.logger.WithError(err).Warn("Invalid initial retry interval, using default")
		initialInterval = 30 * time.Second
	}
	
	maxInterval, err := time.ParseDuration(ws.config.Sync.Retry.MaxInterval)
	if err != nil {
		ws.logger.WithError(err).Warn("Invalid max retry interval, using default")
		maxInterval = 10 * time.Minute
	}
	
	currentInterval := initialInterval
	
	for attempt := 0; attempt <= ws.config.Sync.Retry.MaxRetries; attempt++ {
		if attempt > 0 {
			ws.logger.WithFields(logrus.Fields{
				"attempt": attempt,
				"backoff": currentInterval,
			}).Info("Retrying workload sync after backoff")
			time.Sleep(currentInterval)
		}
		
		// Attempt sync operation
		lastErr = ws.attemptSync()
		if lastErr == nil {
			if attempt > 0 {
				ws.logger.WithField("attempt", attempt).Info("Workload sync succeeded after retry")
			}
			return nil
		}
		
		// Log the failure
		ws.logger.WithFields(logrus.Fields{
			"attempt": attempt + 1,
			"error":   lastErr,
		}).Warn("Workload sync attempt failed")
		
		// Calculate next backoff interval
		if attempt < ws.config.Sync.Retry.MaxRetries {
			currentInterval = time.Duration(float64(currentInterval) * ws.config.Sync.Retry.Multiplier)
			if currentInterval > maxInterval {
				currentInterval = maxInterval
			}
		}
	}
	
	return fmt.Errorf("workload sync failed after %d attempts: %w", ws.config.Sync.Retry.MaxRetries+1, lastErr)
}

// attemptSync performs a single sync attempt
func (ws *WorkloadScheduler) attemptSync() error {
	startTime := time.Now()
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ws.config.Sync.TimeoutSeconds)*time.Second)
	defer cancel()
	
	// Collect workload snapshot
	snapshot, err := ws.collector.CollectWorkloadSnapshot(ctx, ws.config.Namespace)
	if err != nil {
		return fmt.Errorf("failed to collect workload snapshot: %w", err)
	}
	
	// Handle the snapshot based on handler type
	if err := ws.handleWorkloadSnapshot(snapshot, startTime); err != nil {
		return fmt.Errorf("failed to handle workload snapshot: %w", err)
	}
	
	return nil
}

// handleWorkloadSnapshot handles the workload snapshot based on the configured handler
func (ws *WorkloadScheduler) handleWorkloadSnapshot(snapshot *collector.ClusterWorkloadSnapshot, startTime time.Time) error {
	// Check if handler supports workload snapshots
	if workloadHandler, ok := ws.handler.(WorkloadSnapshotHandler); ok {
		// Handler supports workload snapshots natively
		return workloadHandler.HandleWorkloadSnapshot(snapshot, ws.createMetadata(snapshot, startTime))
	}
	
	// Fallback: convert to regular events for backward compatibility
	return ws.handleAsEvents(snapshot)
}

// createMetadata creates sync metadata
func (ws *WorkloadScheduler) createMetadata(snapshot *collector.ClusterWorkloadSnapshot, startTime time.Time) collector.WorkloadSyncMetadata {
	return collector.WorkloadSyncMetadata{
		NodeCount:             len(snapshot.Nodes),
		WorkloadCount:         len(snapshot.Workloads),
		JobCount:              len(snapshot.Jobs),
		NamespaceCount:        len(snapshot.Namespaces),
		CompressionEnabled:    ws.config.Sync.Compression,
		CollectionDurationMs:  int(time.Since(startTime).Milliseconds()),
	}
}

// handleAsEvents converts workload snapshot to individual events for backward compatibility
func (ws *WorkloadScheduler) handleAsEvents(snapshot *collector.ClusterWorkloadSnapshot) error {
	// This is a fallback method for handlers that don't support workload snapshots
	// We create a summary event about the current cluster state
	
	// For now, we'll just log that we're using the fallback method
	ws.logger.Info("Using fallback event handling for workload snapshot")
	
	totalWorkloads := len(snapshot.Workloads)
	totalJobs := len(snapshot.Jobs)
	
	ws.logger.WithFields(logrus.Fields{
		"nodes":      len(snapshot.Nodes),
		"workloads":  totalWorkloads,
		"jobs":       totalJobs,
		"namespaces": len(snapshot.Namespaces),
	}).Info("Workload snapshot summary")
	
	return nil
}

// WorkloadSnapshotHandler interface for handlers that support workload snapshots
type WorkloadSnapshotHandler interface {
	HandleWorkloadSnapshot(snapshot *collector.ClusterWorkloadSnapshot, metadata collector.WorkloadSyncMetadata) error
}

// ValidateConfig validates the sync configuration
func ValidateConfig(config *config.Config) error {
	if !config.Sync.Enabled {
		return nil
	}
	
	if config.Sync.Interval == "" {
		return fmt.Errorf("sync interval cannot be empty")
	}
	
	if _, err := time.ParseDuration(config.Sync.Interval); err != nil {
		return fmt.Errorf("invalid sync interval format: %w", err)
	}
	
	if config.Sync.TimeoutSeconds <= 0 {
		return fmt.Errorf("sync timeout must be positive")
	}
	
	if config.Sync.BatchSize <= 0 {
		return fmt.Errorf("sync batch size must be positive")
	}
	
	// Validate retry configuration
	if config.Sync.Retry.MaxRetries < 0 {
		return fmt.Errorf("retry max retries must be non-negative")
	}
	
	if config.Sync.Retry.InitialInterval != "" {
		if _, err := time.ParseDuration(config.Sync.Retry.InitialInterval); err != nil {
			return fmt.Errorf("invalid retry initial interval format: %w", err)
		}
	}
	
	if config.Sync.Retry.MaxInterval != "" {
		if _, err := time.ParseDuration(config.Sync.Retry.MaxInterval); err != nil {
			return fmt.Errorf("invalid retry max interval format: %w", err)
		}
	}
	
	if config.Sync.Retry.Multiplier < 1.0 {
		return fmt.Errorf("retry multiplier must be >= 1.0")
	}
	
	return nil
}