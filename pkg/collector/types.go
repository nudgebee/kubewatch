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

// ClusterWorkloadSnapshot represents a complete workload snapshot of the cluster
type ClusterWorkloadSnapshot struct {
	Nodes      []NodeInfo      `json:"nodes,omitempty"`
	Workloads  []ServiceInfo   `json:"workloads"`
	Jobs       []JobInfo       `json:"jobs,omitempty"`
	Namespaces []NamespaceInfo `json:"namespaces,omitempty"`
}

// EnvVar represents an environment variable
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Resources represents resource limits and requests
type Resources struct {
	Limits   map[string]string `json:"limits"`
	Requests map[string]string `json:"requests"`
}

// ContainerInfo contains container information
type ContainerInfo struct {
	Name           string                   `json:"name"`
	Image          string                   `json:"image"`
	Env            []EnvVar                 `json:"env"`
	Resources      Resources                `json:"resources"`
	Ports          []int                    `json:"ports"`
	LivenessProbe  map[string]interface{}   `json:"liveness_probe,omitempty"`
	VolumeMounts   []map[string]interface{} `json:"volume_mounts,omitempty"`
	ReadinessProbe map[string]interface{}   `json:"readiness_probe,omitempty"`
}

// VolumeInfo contains volume information
type VolumeInfo struct {
	Name                  string            `json:"name"`
	PersistentVolumeClaim map[string]string `json:"persistent_volume_claim,omitempty"`
}

// OwnerInfo contains owner reference information
type OwnerInfo struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// ConditionInfo contains condition information
type ConditionInfo struct {
	Type   string  `json:"type"`
	Status *string `json:"status,omitempty"`
}

// ServiceConfig contains service configuration details
type ServiceConfig struct {
	Labels         map[string]string        `json:"labels"`
	Containers     []ContainerInfo          `json:"containers"`
	Volumes        []VolumeInfo             `json:"volumes"`
	Owner          []OwnerInfo              `json:"owner"`
	Toleration     []map[string]interface{} `json:"toleration,omitempty"`
	Affinity       map[string]interface{}   `json:"affinity,omitempty"`
	IP             *string                  `json:"ip,omitempty"`
	QosClass       *string                  `json:"qos_class,omitempty"`
	ServiceAccount *string                  `json:"service_account,omitempty"`
	Annotations    map[string]interface{}   `json:"annotations,omitempty"`
	Conditions     []ConditionInfo          `json:"conditions,omitempty"`
}

// ServiceInfo represents all workload types (pod, deployment, statefulset, etc.)
type ServiceInfo struct {
	ResourceVersion int                    `json:"resource_version"`
	Name            string                 `json:"name"`
	ServiceType     string                 `json:"service_type"`
	Namespace       string                 `json:"namespace"`
	Classification  string                 `json:"classification"`
	Deleted         bool                   `json:"deleted"`
	ServiceConfig   *ServiceConfig         `json:"service_config,omitempty"`
	ReadyPods       int                    `json:"ready_pods"`
	TotalPods       int                    `json:"total_pods"`
	IsHelmRelease   *bool                  `json:"is_helm_release,omitempty"`
	Status          *string                `json:"status,omitempty"`
	NodeName        *string                `json:"node_name,omitempty"`
	RestartCount    map[string]int         `json:"restart_count,omitempty"`
	CreationTime    *string                `json:"creation_time,omitempty"`
	StatusDict      map[string]interface{} `json:"status_dict,omitempty"`
}

// NodeInfo contains node information
type NodeInfo struct {
	Name              string                 `json:"name"`
	NodeCreationTime  string                 `json:"node_creation_time"`
	InternalIP        string                 `json:"internal_ip"`
	ExternalIP        string                 `json:"external_ip"`
	Taints            string                 `json:"taints"`
	Conditions        string                 `json:"conditions"`
	MemoryCapacity    int                    `json:"memory_capacity"`    // MB
	MemoryAllocatable int                    `json:"memory_allocatable"` // MB
	MemoryAllocated   int                    `json:"memory_allocated"`   // MB
	CPUCapacity       float64                `json:"cpu_capacity"`
	CPUAllocatable    float64                `json:"cpu_allocatable"`
	CPUAllocated      float64                `json:"cpu_allocated"`
	PodsCount         int                    `json:"pods_count"`
	Pods              string                 `json:"pods"`
	NodeInfo          map[string]interface{} `json:"node_info"`
	Deleted           bool                   `json:"deleted"`
	ResourceVersion   int                    `json:"resource_version"`
	CPULimits         float64                `json:"cpu_limits"`
	MemoryLimits      int                    `json:"memory_limits"`
}

// JobCondition represents job condition
type JobCondition struct {
	Type    string  `json:"type"`
	Message *string `json:"message,omitempty"`
}

// JobStatus represents job status
type JobStatus struct {
	Active           *int           `json:"active,omitempty"`
	Failed           *int           `json:"failed,omitempty"`
	Succeeded        *int           `json:"succeeded,omitempty"`
	CompletionTime   *string        `json:"completion_time,omitempty"`
	FailedTime       *string        `json:"failed_time,omitempty"`
	Conditions       []JobCondition `json:"conditions"`
	LastScheduleTime *string        `json:"last_schedule_time,omitempty"`
}

// JobContainer represents container information in a job
type JobContainer struct {
	Name           string                   `json:"name"`
	Image          string                   `json:"image"`
	Env            []EnvVar                 `json:"env"`
	Resources      Resources                `json:"resources"`
	Ports          []int                    `json:"ports"`
	LivenessProbe  map[string]interface{}   `json:"liveness_probe,omitempty"`
	VolumeMounts   []map[string]interface{} `json:"volume_mounts,omitempty"`
	ReadinessProbe map[string]interface{}   `json:"readiness_probe,omitempty"`
	CPUReq         float64                  `json:"cpu_req"`
	CPULimit       float64                  `json:"cpu_limit"`
	MemReq         int                      `json:"mem_req"`
	MemLimit       int                      `json:"mem_limit"`
}

// JobData represents job data
type JobData struct {
	CompletionTime *string        `json:"completion_time,omitempty"`
	StartTime      *string        `json:"start_time,omitempty"`
	Duration       *string        `json:"duration,omitempty"`
	Parallelism    *int           `json:"parallelism,omitempty"`
	BackoffLimit   *int           `json:"backoff_limit,omitempty"`
	Containers     []JobContainer `json:"containers"`
}

// JobInfo contains job information
type JobInfo struct {
	Name        string    `json:"name"`
	Namespace   string    `json:"namespace"`
	Type        string    `json:"type"`
	CreatedAt   string    `json:"created_at"`
	Deleted     bool      `json:"deleted"`
	CPUReq      float64   `json:"cpu_req"`
	MemReq      int       `json:"mem_req"`
	Completions int       `json:"completions"`
	Status      JobStatus `json:"status"`
	JobData     JobData   `json:"job_data"`
}

// NamespaceInfo contains namespace information
type NamespaceInfo struct {
	Name           string  `json:"name"`
	Deleted        bool    `json:"deleted"`
	WorkloadCount  int     `json:"workload_count"`
	PodCount       int     `json:"pod_count"`
	CreationTime   *string `json:"creation_time,omitempty"`
}

// WorkloadSyncMetadata contains metadata about the sync operation
type WorkloadSyncMetadata struct {
	NodeCount            int  `json:"nodeCount"`
	WorkloadCount        int  `json:"workloadCount"`
	JobCount             int  `json:"jobCount"`
	NamespaceCount       int  `json:"namespaceCount"`
	CompressionEnabled   bool `json:"compressionEnabled"`
	CollectionDurationMs int  `json:"collectionDurationMs"`
}

// Constants for service types
const (
	SERVICE_TYPE_POD         = "Pod"
	SERVICE_TYPE_DEPLOYMENT  = "Deployment"
	SERVICE_TYPE_STATEFULSET = "StatefulSet"
	SERVICE_TYPE_DAEMONSET   = "DaemonSet"
	SERVICE_TYPE_REPLICASET  = "ReplicaSet"
	SERVICE_TYPE_SERVICE     = "Service"
	SERVICE_TYPE_JOB         = "Job"
)
