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

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/sirupsen/logrus"
	
	apps_v1 "k8s.io/api/apps/v1"
	batch_v1 "k8s.io/api/batch/v1"
	api_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// WorkloadCollector collects workload snapshot data from Kubernetes cluster
type WorkloadCollector struct {
	client kubernetes.Interface
	config *config.SyncConfig
	logger *logrus.Entry
}

// NewWorkloadCollector creates a new workload collector
func NewWorkloadCollector(client kubernetes.Interface, config *config.SyncConfig) *WorkloadCollector {
	return &WorkloadCollector{
		client: client,
		config: config,
		logger: logrus.WithField("component", "workload-collector"),
	}
}

// CollectWorkloadSnapshot collects a complete workload snapshot
func (w *WorkloadCollector) CollectWorkloadSnapshot(ctx context.Context, namespace string) (*ClusterWorkloadSnapshot, error) {
	w.logger.Info("Starting workload snapshot collection")
	startTime := time.Now()
	
	snapshot := &ClusterWorkloadSnapshot{
		Workloads:  []ServiceInfo{},
		Jobs:       []JobInfo{},
		Namespaces: []NamespaceInfo{},
	}
	
	// Use wait group for concurrent collection
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, 10)
	
	// Collect workloads (using ServiceInfo for all workload types)
	wg.Add(5)
	go w.collectDeployments(ctx, namespace, &snapshot.Workloads, &mu, &wg, errChan)
	go w.collectDaemonSets(ctx, namespace, &snapshot.Workloads, &mu, &wg, errChan)
	go w.collectStatefulSets(ctx, namespace, &snapshot.Workloads, &mu, &wg, errChan)
	go w.collectPods(ctx, namespace, &snapshot.Workloads, &mu, &wg, errChan)
	go w.collectServices(ctx, namespace, &snapshot.Workloads, &mu, &wg, errChan)
	
	// Collect jobs separately
	wg.Add(1)
	go w.collectJobs(ctx, namespace, &snapshot.Jobs, &mu, &wg, errChan)
	
	// Collect namespaces
	wg.Add(1)
	go w.collectNamespaces(ctx, &snapshot.Namespaces, &mu, &wg, errChan)
	
	// Conditionally collect nodes
	if w.config.WorkloadSnapshot.IncludeNodes {
		wg.Add(1)
		go w.collectNodes(ctx, &snapshot.Nodes, &mu, &wg, errChan)
	}
	
	// Wait for all collections to complete
	wg.Wait()
	close(errChan)
	
	// Check for errors
	for err := range errChan {
		if err != nil {
			w.logger.WithError(err).Error("Error during workload collection")
			return nil, err
		}
	}
	
	duration := time.Since(startTime)
	w.logger.WithField("duration", duration).Info("Workload snapshot collection completed")
	
	return snapshot, nil
}

// collectDeployments collects deployments as ServiceInfo
func (w *WorkloadCollector) collectDeployments(ctx context.Context, namespace string, workloads *[]ServiceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	deployments, err := w.client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, dep := range deployments.Items {
		serviceInfo := w.convertDeploymentToServiceInfo(&dep)
		
		mu.Lock()
		*workloads = append(*workloads, serviceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(deployments.Items)).Debug("Collected deployments")
}

// collectDaemonSets collects daemonsets as ServiceInfo
func (w *WorkloadCollector) collectDaemonSets(ctx context.Context, namespace string, workloads *[]ServiceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	daemonSets, err := w.client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, ds := range daemonSets.Items {
		serviceInfo := w.convertDaemonSetToServiceInfo(&ds)
		
		mu.Lock()
		*workloads = append(*workloads, serviceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(daemonSets.Items)).Debug("Collected daemonsets")
}

// collectStatefulSets collects statefulsets as ServiceInfo
func (w *WorkloadCollector) collectStatefulSets(ctx context.Context, namespace string, workloads *[]ServiceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	statefulSets, err := w.client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, ss := range statefulSets.Items {
		serviceInfo := w.convertStatefulSetToServiceInfo(&ss)
		
		mu.Lock()
		*workloads = append(*workloads, serviceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(statefulSets.Items)).Debug("Collected statefulsets")
}

// collectPods collects pods as ServiceInfo
func (w *WorkloadCollector) collectPods(ctx context.Context, namespace string, workloads *[]ServiceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	pods, err := w.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, pod := range pods.Items {
		serviceInfo := w.convertPodToServiceInfo(&pod)
		
		mu.Lock()
		*workloads = append(*workloads, serviceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(pods.Items)).Debug("Collected pods")
}

// collectServices collects services as ServiceInfo
func (w *WorkloadCollector) collectServices(ctx context.Context, namespace string, workloads *[]ServiceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	services, err := w.client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, svc := range services.Items {
		serviceInfo := w.convertServiceToServiceInfo(&svc)
		
		mu.Lock()
		*workloads = append(*workloads, serviceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(services.Items)).Debug("Collected services")
}

// collectJobs collects jobs as JobInfo
func (w *WorkloadCollector) collectJobs(ctx context.Context, namespace string, jobs *[]JobInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	jobList, err := w.client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, job := range jobList.Items {
		jobInfo := w.convertJobToJobInfo(&job)
		
		mu.Lock()
		*jobs = append(*jobs, jobInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(jobList.Items)).Debug("Collected jobs")
}

// collectNodes collects nodes as NodeInfo
func (w *WorkloadCollector) collectNodes(ctx context.Context, nodes *[]NodeInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	nodeList, err := w.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, node := range nodeList.Items {
		nodeInfo := w.convertNodeToNodeInfo(&node)
		
		mu.Lock()
		*nodes = append(*nodes, nodeInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(nodeList.Items)).Debug("Collected nodes")
}

// collectNamespaces collects namespaces as NamespaceInfo
func (w *WorkloadCollector) collectNamespaces(ctx context.Context, namespaces *[]NamespaceInfo, mu *sync.Mutex, wg *sync.WaitGroup, errChan chan error) {
	defer wg.Done()
	
	namespaceList, err := w.client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		errChan <- err
		return
	}
	
	for _, ns := range namespaceList.Items {
		namespaceInfo := w.convertNamespaceToNamespaceInfo(&ns)
		
		mu.Lock()
		*namespaces = append(*namespaces, namespaceInfo)
		mu.Unlock()
	}
	
	w.logger.WithField("count", len(namespaceList.Items)).Debug("Collected namespaces")
}

// convertDeploymentToServiceInfo converts a deployment to ServiceInfo
func (w *WorkloadCollector) convertDeploymentToServiceInfo(dep *apps_v1.Deployment) ServiceInfo {
	resourceVersion := w.parseResourceVersion(dep.ResourceVersion)
	
	// Create service config with default values
	serviceConfig := &ServiceConfig{
		Labels:      w.ensureLabels(dep.Labels),
		Containers:  w.extractContainers(dep.Spec.Template.Spec.Containers),
		Volumes:     w.extractVolumes(dep.Spec.Template.Spec.Volumes),
		Owner:       w.extractOwners(dep.OwnerReferences),
		Annotations: w.convertAnnotations(dep.Annotations),
	}
	
	status := "Unknown"
	if dep.Status.ReadyReplicas == *dep.Spec.Replicas {
		status = "Ready"
	} else if dep.Status.ReadyReplicas > 0 {
		status = "Partial"
	} else {
		status = "NotReady"
	}
	
	isHelmRelease := w.isHelmRelease(dep.Labels)
	creationTime := dep.CreationTimestamp.Time.Format(time.RFC3339)
	
	return ServiceInfo{
		ResourceVersion: resourceVersion,
		Name:           dep.Name,
		ServiceType:    SERVICE_TYPE_DEPLOYMENT,
		Namespace:      dep.Namespace,
		Classification: "deployment",
		Deleted:        false,
		ServiceConfig:  serviceConfig,
		ReadyPods:      int(dep.Status.ReadyReplicas),
		TotalPods:      int(*dep.Spec.Replicas),
		IsHelmRelease:  isHelmRelease,
		Status:         &status,
		CreationTime:   &creationTime,
	}
}

// convertDaemonSetToServiceInfo converts a daemonset to ServiceInfo
func (w *WorkloadCollector) convertDaemonSetToServiceInfo(ds *apps_v1.DaemonSet) ServiceInfo {
	resourceVersion := w.parseResourceVersion(ds.ResourceVersion)
	
	serviceConfig := &ServiceConfig{
		Labels:      w.ensureLabels(ds.Labels),
		Containers:  w.extractContainers(ds.Spec.Template.Spec.Containers),
		Volumes:     w.extractVolumes(ds.Spec.Template.Spec.Volumes),
		Owner:       w.extractOwners(ds.OwnerReferences),
		Annotations: w.convertAnnotations(ds.Annotations),
	}
	
	status := "Unknown"
	if ds.Status.NumberReady == ds.Status.DesiredNumberScheduled {
		status = "Ready"
	} else if ds.Status.NumberReady > 0 {
		status = "Partial"
	} else {
		status = "NotReady"
	}
	
	isHelmRelease := w.isHelmRelease(ds.Labels)
	creationTime := ds.CreationTimestamp.Time.Format(time.RFC3339)
	
	return ServiceInfo{
		ResourceVersion: resourceVersion,
		Name:           ds.Name,
		ServiceType:    SERVICE_TYPE_DAEMONSET,
		Namespace:      ds.Namespace,
		Classification: "daemonset",
		Deleted:        false,
		ServiceConfig:  serviceConfig,
		ReadyPods:      int(ds.Status.NumberReady),
		TotalPods:      int(ds.Status.DesiredNumberScheduled),
		IsHelmRelease:  isHelmRelease,
		Status:         &status,
		CreationTime:   &creationTime,
	}
}

// convertStatefulSetToServiceInfo converts a statefulset to ServiceInfo
func (w *WorkloadCollector) convertStatefulSetToServiceInfo(ss *apps_v1.StatefulSet) ServiceInfo {
	resourceVersion := w.parseResourceVersion(ss.ResourceVersion)
	
	serviceConfig := &ServiceConfig{
		Labels:      w.ensureLabels(ss.Labels),
		Containers:  w.extractContainers(ss.Spec.Template.Spec.Containers),
		Volumes:     w.extractVolumes(ss.Spec.Template.Spec.Volumes),
		Owner:       w.extractOwners(ss.OwnerReferences),
		Annotations: w.convertAnnotations(ss.Annotations),
	}
	
	status := "Unknown"
	if ss.Status.ReadyReplicas == *ss.Spec.Replicas {
		status = "Ready"
	} else if ss.Status.ReadyReplicas > 0 {
		status = "Partial"
	} else {
		status = "NotReady"
	}
	
	isHelmRelease := w.isHelmRelease(ss.Labels)
	creationTime := ss.CreationTimestamp.Time.Format(time.RFC3339)
	
	return ServiceInfo{
		ResourceVersion: resourceVersion,
		Name:           ss.Name,
		ServiceType:    SERVICE_TYPE_STATEFULSET,
		Namespace:      ss.Namespace,
		Classification: "statefulset",
		Deleted:        false,
		ServiceConfig:  serviceConfig,
		ReadyPods:      int(ss.Status.ReadyReplicas),
		TotalPods:      int(*ss.Spec.Replicas),
		IsHelmRelease:  isHelmRelease,
		Status:         &status,
		CreationTime:   &creationTime,
	}
}

// convertPodToServiceInfo converts a pod to ServiceInfo
func (w *WorkloadCollector) convertPodToServiceInfo(pod *api_v1.Pod) ServiceInfo {
	resourceVersion := w.parseResourceVersion(pod.ResourceVersion)
	
	// Convert QOSClass to string safely
	var qosClass *string
	if pod.Status.QOSClass != "" {
		qosStr := string(pod.Status.QOSClass)
		qosClass = &qosStr
	}
	
	serviceConfig := &ServiceConfig{
		Labels:         w.ensureLabels(pod.Labels),
		Containers:     w.extractContainers(pod.Spec.Containers),
		Volumes:        w.extractVolumes(pod.Spec.Volumes),
		Owner:          w.extractOwners(pod.OwnerReferences),
		Annotations:    w.convertAnnotations(pod.Annotations),
		IP:             &pod.Status.PodIP,
		QosClass:       qosClass,
		ServiceAccount: &pod.Spec.ServiceAccountName,
	}
	
	status := string(pod.Status.Phase)
	isHelmRelease := w.isHelmRelease(pod.Labels)
	creationTime := pod.CreationTimestamp.Time.Format(time.RFC3339)
	
	// Calculate restart count
	restartCount := make(map[string]int)
	for _, containerStatus := range pod.Status.ContainerStatuses {
		restartCount[containerStatus.Name] = int(containerStatus.RestartCount)
	}
	
	return ServiceInfo{
		ResourceVersion: resourceVersion,
		Name:           pod.Name,
		ServiceType:    SERVICE_TYPE_POD,
		Namespace:      pod.Namespace,
		Classification: "pod",
		Deleted:        false,
		ServiceConfig:  serviceConfig,
		ReadyPods:      w.isPodReady(pod),
		TotalPods:      1,
		IsHelmRelease:  isHelmRelease,
		Status:         &status,
		NodeName:       &pod.Spec.NodeName,
		RestartCount:   restartCount,
		CreationTime:   &creationTime,
	}
}

// convertServiceToServiceInfo converts a service to ServiceInfo
func (w *WorkloadCollector) convertServiceToServiceInfo(svc *api_v1.Service) ServiceInfo {
	resourceVersion := w.parseResourceVersion(svc.ResourceVersion)
	
	serviceConfig := &ServiceConfig{
		Labels:      w.ensureLabels(svc.Labels),
		Containers:  w.extractContainers(nil), // Services don't have containers
		Volumes:     w.extractVolumes(nil),    // Services don't have volumes
		Owner:       w.extractOwners(svc.OwnerReferences),
		Annotations: w.convertAnnotations(svc.Annotations),
	}
	
	status := "Active"
	isHelmRelease := w.isHelmRelease(svc.Labels)
	creationTime := svc.CreationTimestamp.Time.Format(time.RFC3339)
	
	return ServiceInfo{
		ResourceVersion: resourceVersion,
		Name:           svc.Name,
		ServiceType:    SERVICE_TYPE_SERVICE,
		Namespace:      svc.Namespace,
		Classification: "service",
		Deleted:        false,
		ServiceConfig:  serviceConfig,
		ReadyPods:      0, // Services don't have direct pod counts
		TotalPods:      0,
		IsHelmRelease:  isHelmRelease,
		Status:         &status,
		CreationTime:   &creationTime,
	}
}

// convertJobToJobInfo converts a job to JobInfo
func (w *WorkloadCollector) convertJobToJobInfo(job *batch_v1.Job) JobInfo {
	creationTime := job.CreationTimestamp.Time.Format(time.RFC3339)
	
	// Calculate resource requirements
	cpuReq := 0.0
	memReq := 0
	
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Resources.Requests != nil {
			if cpu := container.Resources.Requests.Cpu(); cpu != nil {
				cpuReq += cpu.AsApproximateFloat64()
			}
			if memory := container.Resources.Requests.Memory(); memory != nil {
				memReq += int(memory.Value() / (1024 * 1024)) // Convert to MB
			}
		}
	}
	
	completions := 1
	if job.Spec.Completions != nil {
		completions = int(*job.Spec.Completions)
	}
	
	// Create job containers
	jobContainers := w.extractJobContainers(job.Spec.Template.Spec.Containers)
	
	// Create job data
	jobData := JobData{
		Containers: jobContainers,
	}
	if job.Status.StartTime != nil {
		startTime := job.Status.StartTime.Time.Format(time.RFC3339)
		jobData.StartTime = &startTime
	}
	if job.Status.CompletionTime != nil {
		completionTime := job.Status.CompletionTime.Time.Format(time.RFC3339)
		jobData.CompletionTime = &completionTime
		
		// Calculate duration if both start and completion times are available
		if job.Status.StartTime != nil {
			duration := job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
			durationStr := duration.String()
			jobData.Duration = &durationStr
		}
	}
	if job.Spec.Parallelism != nil {
		parallelism := int(*job.Spec.Parallelism)
		jobData.Parallelism = &parallelism
	}
	if job.Spec.BackoffLimit != nil {
		backoffLimit := int(*job.Spec.BackoffLimit)
		jobData.BackoffLimit = &backoffLimit
	}
	
	// Create job conditions
	jobConditions := w.extractJobConditions(job.Status.Conditions)
	
	// Create job status with proper pointer values
	active := int(job.Status.Active)
	succeeded := int(job.Status.Succeeded)
	failed := int(job.Status.Failed)
	
	jobStatus := JobStatus{
		Active:     &active,
		Succeeded:  &succeeded,
		Failed:     &failed,
		Conditions: jobConditions,
	}
	
	if job.Status.CompletionTime != nil {
		completionTime := job.Status.CompletionTime.Time.Format(time.RFC3339)
		jobStatus.CompletionTime = &completionTime
	}
	
	// Handle failed time from conditions
	for _, condition := range job.Status.Conditions {
		if condition.Type == batch_v1.JobFailed && condition.Status == "True" {
			failedTime := condition.LastTransitionTime.Time.Format(time.RFC3339)
			jobStatus.FailedTime = &failedTime
			break
		}
	}
	
	return JobInfo{
		Name:        job.Name,
		Namespace:   job.Namespace,
		Type:        SERVICE_TYPE_JOB,
		CreatedAt:   creationTime,
		Deleted:     false,
		CPUReq:      cpuReq,
		MemReq:      memReq,
		Completions: completions,
		Status:      jobStatus,
		JobData:     jobData,
	}
}

// convertNodeToNodeInfo converts a node to NodeInfo
func (w *WorkloadCollector) convertNodeToNodeInfo(node *api_v1.Node) NodeInfo {
	creationTime := node.CreationTimestamp.Time.Format(time.RFC3339)
	
	// Extract IPs
	internalIP := ""
	externalIP := ""
	for _, addr := range node.Status.Addresses {
		if addr.Type == api_v1.NodeInternalIP {
			internalIP = addr.Address
		} else if addr.Type == api_v1.NodeExternalIP {
			externalIP = addr.Address
		}
	}
	
	// Extract capacity and allocatable
	memoryCapacity := w.parseMemoryToMB(node.Status.Capacity.Memory())
	memoryAllocatable := w.parseMemoryToMB(node.Status.Allocatable.Memory())
	cpuCapacity := node.Status.Capacity.Cpu().AsApproximateFloat64()
	cpuAllocatable := node.Status.Allocatable.Cpu().AsApproximateFloat64()
	
	// Build conditions string
	conditions := ""
	for _, condition := range node.Status.Conditions {
		if conditions != "" {
			conditions += ","
		}
		conditions += fmt.Sprintf("%s=%s", condition.Type, condition.Status)
	}
	
	// Build taints string
	taints := ""
	for _, taint := range node.Spec.Taints {
		if taints != "" {
			taints += ","
		}
		taints += fmt.Sprintf("%s=%s:%s", taint.Key, taint.Value, taint.Effect)
	}
	
	// Convert node info to map
	nodeInfoMap := make(map[string]interface{})
	nodeInfoBytes, _ := json.Marshal(node.Status.NodeInfo)
	json.Unmarshal(nodeInfoBytes, &nodeInfoMap)
	
	return NodeInfo{
		Name:               node.Name,
		NodeCreationTime:   creationTime,
		InternalIP:         internalIP,
		ExternalIP:         externalIP,
		Taints:             taints,
		Conditions:         conditions,
		MemoryCapacity:     memoryCapacity,
		MemoryAllocatable:  memoryAllocatable,
		MemoryAllocated:    0, // Would need additional calculation
		CPUCapacity:        cpuCapacity,
		CPUAllocatable:     cpuAllocatable,
		CPUAllocated:       0, // Would need additional calculation
		PodsCount:          0, // Would need additional calculation
		Pods:               "",
		NodeInfo:           nodeInfoMap,
		Deleted:            false,
		ResourceVersion:    w.parseResourceVersion(node.ResourceVersion),
		CPULimits:          0,
		MemoryLimits:       0,
	}
}

// Helper functions

func (w *WorkloadCollector) ensureLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return make(map[string]string)
	}
	return labels
}

func (w *WorkloadCollector) parseResourceVersion(version string) int {
	if version == "" {
		return 0
	}
	if v, err := strconv.Atoi(version); err == nil {
		return v
	}
	return 0
}

func (w *WorkloadCollector) isHelmRelease(labels map[string]string) *bool {
	if labels == nil {
		return nil
	}
	if managedBy, exists := labels["app.kubernetes.io/managed-by"]; exists {
		helmRelease := managedBy == "Helm"
		return &helmRelease
	}
	return nil
}

func (w *WorkloadCollector) extractContainers(containers []api_v1.Container) []ContainerInfo {
	if containers == nil {
		return []ContainerInfo{}
	}
	result := make([]ContainerInfo, len(containers))
	for i, container := range containers {
		result[i] = ContainerInfo{
			Name:  container.Name,
			Image: container.Image,
			Env:   w.extractEnvVars(container.Env),
			Resources: Resources{
				Limits:   w.extractResourceMap(container.Resources.Limits),
				Requests: w.extractResourceMap(container.Resources.Requests),
			},
			Ports: w.extractPorts(container.Ports),
		}
	}
	return result
}

func (w *WorkloadCollector) extractEnvVars(envVars []api_v1.EnvVar) []EnvVar {
	result := make([]EnvVar, 0, len(envVars))
	for _, env := range envVars {
		if env.Name != "" && env.Value != "" {
			result = append(result, EnvVar{
				Name:  env.Name,
				Value: env.Value,
			})
		}
	}
	return result
}

func (w *WorkloadCollector) extractResourceMap(resources api_v1.ResourceList) map[string]string {
	result := make(map[string]string)
	if resources != nil {
		for key, value := range resources {
			result[string(key)] = value.String()
		}
	}
	return result
}

func (w *WorkloadCollector) extractPorts(ports []api_v1.ContainerPort) []int {
	result := make([]int, len(ports))
	for i, port := range ports {
		result[i] = int(port.ContainerPort)
	}
	return result
}

func (w *WorkloadCollector) extractVolumes(volumes []api_v1.Volume) []VolumeInfo {
	if volumes == nil {
		return []VolumeInfo{}
	}
	result := make([]VolumeInfo, len(volumes))
	for i, volume := range volumes {
		volumeInfo := VolumeInfo{
			Name: volume.Name,
		}
		if volume.PersistentVolumeClaim != nil {
			volumeInfo.PersistentVolumeClaim = map[string]string{
				"claim_name": volume.PersistentVolumeClaim.ClaimName,
			}
		}
		result[i] = volumeInfo
	}
	return result
}

func (w *WorkloadCollector) extractOwners(owners []metav1.OwnerReference) []OwnerInfo {
	if owners == nil {
		return []OwnerInfo{}
	}
	result := make([]OwnerInfo, len(owners))
	for i, owner := range owners {
		result[i] = OwnerInfo{
			Kind: owner.Kind,
			Name: owner.Name,
		}
	}
	return result
}

func (w *WorkloadCollector) convertAnnotations(annotations map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	if annotations != nil {
		for key, value := range annotations {
			result[key] = value
		}
	}
	return result
}

func (w *WorkloadCollector) isPodReady(pod *api_v1.Pod) int {
	if pod.Status.Phase == api_v1.PodRunning {
		for _, condition := range pod.Status.Conditions {
			if condition.Type == api_v1.PodReady && condition.Status == api_v1.ConditionTrue {
				return 1
			}
		}
	}
	return 0
}

func (w *WorkloadCollector) parseMemoryToMB(quantity *resource.Quantity) int {
	if quantity == nil {
		return 0
	}
	return int(quantity.Value() / (1024 * 1024))
}

// extractJobContainers extracts containers from job spec
func (w *WorkloadCollector) extractJobContainers(containers []api_v1.Container) []JobContainer {
	if containers == nil {
		return []JobContainer{}
	}
	result := make([]JobContainer, len(containers))
	for i, container := range containers {
		// Extract resource requirements
		cpuReq := 0.0
		cpuLimit := 0.0
		memReq := 0
		memLimit := 0
		
		if container.Resources.Requests != nil {
			if cpu := container.Resources.Requests.Cpu(); cpu != nil {
				cpuReq = cpu.AsApproximateFloat64()
			}
			if memory := container.Resources.Requests.Memory(); memory != nil {
				memReq = int(memory.Value() / (1024 * 1024)) // Convert to MB
			}
		}
		
		if container.Resources.Limits != nil {
			if cpu := container.Resources.Limits.Cpu(); cpu != nil {
				cpuLimit = cpu.AsApproximateFloat64()
			}
			if memory := container.Resources.Limits.Memory(); memory != nil {
				memLimit = int(memory.Value() / (1024 * 1024)) // Convert to MB
			}
		}
		
		result[i] = JobContainer{
			Name:  container.Name,
			Image: container.Image,
			Env:   w.extractEnvVars(container.Env),
			Resources: Resources{
				Limits:   w.extractResourceMap(container.Resources.Limits),
				Requests: w.extractResourceMap(container.Resources.Requests),
			},
			Ports:    w.extractPorts(container.Ports),
			CPUReq:   cpuReq,
			CPULimit: cpuLimit,
			MemReq:   memReq,
			MemLimit: memLimit,
		}
	}
	return result
}

// extractJobConditions extracts job conditions
func (w *WorkloadCollector) extractJobConditions(conditions []batch_v1.JobCondition) []JobCondition {
	if conditions == nil {
		return []JobCondition{}
	}
	result := make([]JobCondition, len(conditions))
	for i, condition := range conditions {
		result[i] = JobCondition{
			Type:    string(condition.Type),
			Message: &condition.Message,
		}
	}
	return result
}

// convertNamespaceToNamespaceInfo converts a namespace to NamespaceInfo
func (w *WorkloadCollector) convertNamespaceToNamespaceInfo(ns *api_v1.Namespace) NamespaceInfo {
	creationTime := ns.CreationTimestamp.Time.Format(time.RFC3339)
	
	return NamespaceInfo{
		Name:         ns.Name,
		Deleted:      false,
		WorkloadCount: 0, // Would need additional calculation across workloads
		PodCount:     0,  // Would need additional calculation
		CreationTime: &creationTime,
	}
}