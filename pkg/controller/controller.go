/*
Copyright 2016 Skippbox, Ltd.

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

package controller

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/bitnami-labs/kubewatch/pkg/event"
	"github.com/bitnami-labs/kubewatch/pkg/handlers"
	"github.com/bitnami-labs/kubewatch/pkg/scheduler"
	"github.com/bitnami-labs/kubewatch/pkg/utils"
	"github.com/sirupsen/logrus"

	apps_v1 "k8s.io/api/apps/v1"
	autoscaling_v1 "k8s.io/api/autoscaling/v1"
	batch_v1 "k8s.io/api/batch/v1"
	api_v1 "k8s.io/api/core/v1"
	events_v1 "k8s.io/api/events/v1"
	networking_v1 "k8s.io/api/networking/v1"
	rbac_v1 "k8s.io/api/rbac/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const V1 = "v1"
const AUTOSCALING_V1 = "autoscaling/v1"
const APPS_V1 = "apps/v1"
const BATCH_V1 = "batch/v1"
const RBAC_V1 = "rbac.authorization.k8s.io/v1"
const NETWORKING_V1 = "networking.k8s.io/v1"
const EVENTS_V1 = "events.k8s.io/v1"

var serverStartTime time.Time

// Event indicate the informerEvent
type Event struct {
	key          string
	eventType    string
	namespace    string
	resourceType string
	apiVersion   string
	obj          runtime.Object
	oldObj       runtime.Object
}

// Controller object
type Controller struct {
	logger       *logrus.Entry
	clientset    kubernetes.Interface
	queue        workqueue.TypedInterface[string] // object keys; events wait in pending
	pending      *pendingEvents
	informer     cache.SharedIndexInformer
	eventHandler handlers.Handler
	resourceType string
	apiVersion   string
	workers      int
}

// queueOptions sizes every resource controller's send queue.
type queueOptions struct {
	workers    int // concurrent senders per resource type
	maxPending int // objects allowed to wait per resource type; <= 0 unbounded
}

func objName(obj interface{}) string {
	return reflect.TypeOf(obj).Name()
}

// TODO: we don't need the informer to be indexed
// Start prepares watchers and run their controllers, then waits for process termination signals
func Start(conf *config.Config, eventHandler handlers.Handler) {
	var kubeClient kubernetes.Interface
	var dynamicClient dynamic.Interface

	kubewatchEventsMetrics := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kubewatch_events_total",
			Help: "The total number of Kubernetes events observed by Kubewatch, labeled by resource and event type",
		},
		[]string{"resourceType", "eventType"},
	)

	conf.SetQueueDefaults()
	opts := queueOptions{workers: conf.Queue.Workers, maxPending: conf.Queue.MaxPending}
	logrus.Infof("Send queue: %d workers per resource type, at most %d objects waiting per type (negative = unbounded)",
		opts.workers, opts.maxPending)

	if _, err := rest.InClusterConfig(); err != nil {
		kubeClient = utils.GetClientOutOfCluster()
		dynamicClient = utils.GetDynamicClientOutOfCluster()
	} else {
		kubeClient = utils.GetClient()
		dynamicClient = utils.GetDynamicClient()
	}

	// User Configured Events
	if conf.Resource.CoreEvent {
		allCoreEventsInformer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					options.FieldSelector = ""
					return kubeClient.CoreV1().Events(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					options.FieldSelector = ""
					return kubeClient.CoreV1().Events(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.Event{},
			0, //Skip resync
			cache.Indexers{},
		)

		allCoreEventsController := newResourceController(kubeClient, eventHandler, allCoreEventsInformer, objName(api_v1.Event{}), V1, kubewatchEventsMetrics, opts)
		stopAllCoreEventsCh := make(chan struct{})
		defer close(stopAllCoreEventsCh)

		go allCoreEventsController.Run(stopAllCoreEventsCh)
	}

	if conf.Resource.Event {
		allEventsInformer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					options.FieldSelector = ""
					return kubeClient.EventsV1().Events(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					options.FieldSelector = ""
					return kubeClient.EventsV1().Events(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&events_v1.Event{},
			0, //Skip resync
			cache.Indexers{},
		)

		allEventsController := newResourceController(kubeClient, eventHandler, allEventsInformer, objName(events_v1.Event{}), EVENTS_V1, kubewatchEventsMetrics, opts)
		stopAllEventsCh := make(chan struct{})
		defer close(stopAllEventsCh)

		go allEventsController.Run(stopAllEventsCh)
	}

	if conf.Resource.Pod {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().Pods(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().Pods(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.Pod{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.Pod{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.HPA {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.AutoscalingV1().HorizontalPodAutoscalers(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.AutoscalingV1().HorizontalPodAutoscalers(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&autoscaling_v1.HorizontalPodAutoscaler{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(autoscaling_v1.HorizontalPodAutoscaler{}), AUTOSCALING_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)

	}

	if conf.Resource.DaemonSet {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.AppsV1().DaemonSets(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.AppsV1().DaemonSets(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&apps_v1.DaemonSet{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(apps_v1.DaemonSet{}), APPS_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.StatefulSet {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.AppsV1().StatefulSets(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.AppsV1().StatefulSets(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&apps_v1.StatefulSet{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(apps_v1.StatefulSet{}), APPS_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ReplicaSet {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.AppsV1().ReplicaSets(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.AppsV1().ReplicaSets(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&apps_v1.ReplicaSet{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(apps_v1.ReplicaSet{}), APPS_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Services {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().Services(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().Services(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.Service{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.Service{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Deployment {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.AppsV1().Deployments(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.AppsV1().Deployments(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&apps_v1.Deployment{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(apps_v1.Deployment{}), APPS_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Namespace {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().Namespaces().List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().Namespaces().Watch(context.Background(), options)
				},
			},
			&api_v1.Namespace{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.Namespace{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ReplicationController {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().ReplicationControllers(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().ReplicationControllers(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.ReplicationController{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.ReplicationController{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Job {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.BatchV1().Jobs(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.BatchV1().Jobs(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&batch_v1.Job{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(batch_v1.Job{}), BATCH_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Node {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().Nodes().List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().Nodes().Watch(context.Background(), options)
				},
			},
			&api_v1.Node{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.Node{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ServiceAccount {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().ServiceAccounts(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().ServiceAccounts(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.ServiceAccount{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.ServiceAccount{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ClusterRole {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.RbacV1().ClusterRoles().List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.RbacV1().ClusterRoles().Watch(context.Background(), options)
				},
			},
			&rbac_v1.ClusterRole{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(rbac_v1.ClusterRole{}), RBAC_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ClusterRoleBinding {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.RbacV1().ClusterRoleBindings().List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.RbacV1().ClusterRoleBindings().Watch(context.Background(), options)
				},
			},
			&rbac_v1.ClusterRoleBinding{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(rbac_v1.ClusterRoleBinding{}), RBAC_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.PersistentVolume {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().PersistentVolumes().List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().PersistentVolumes().Watch(context.Background(), options)
				},
			},
			&api_v1.PersistentVolume{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.PersistentVolume{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Secret {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().Secrets(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().Secrets(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.Secret{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.Secret{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.ConfigMap {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.CoreV1().ConfigMaps(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.CoreV1().ConfigMaps(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&api_v1.ConfigMap{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(api_v1.ConfigMap{}), V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	if conf.Resource.Ingress {
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return kubeClient.NetworkingV1().Ingresses(conf.Namespace).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return kubeClient.NetworkingV1().Ingresses(conf.Namespace).Watch(context.Background(), options)
				},
			},
			&networking_v1.Ingress{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, objName(networking_v1.Ingress{}), NETWORKING_V1, kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}

	for _, curRes := range conf.CustomResources {
		crd := curRes
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					return dynamicClient.Resource(schema.GroupVersionResource{
						Group:    crd.Group,
						Version:  crd.Version,
						Resource: crd.Resource,
					}).List(context.Background(), options)
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					return dynamicClient.Resource(schema.GroupVersionResource{
						Group:    crd.Group,
						Version:  crd.Version,
						Resource: crd.Resource,
					}).Watch(context.Background(), options)
				},
			},
			&unstructured.Unstructured{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, crd.Resource, fmt.Sprintf("%s/%s", crd.Group, crd.Version), kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)

		go c.Run(stopCh)
	}
	if conf.Resource.Rollout {
		rolloutCRD := "rollouts.argoproj.io"
		rolloutVersion := "v1alpha1"
		
		logrus.WithFields(logrus.Fields{
			"pkg":      "kubewatch",
			"crd":      rolloutCRD,
			"version":  rolloutVersion,
			"resource": "rollouts",
		}).Info("Checking Argo Rollout CRD availability")
		
		flag, err := utils.CheckCRDExists(dynamicClient, rolloutCRD, rolloutVersion)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"pkg":      "kubewatch",
				"crd":      rolloutCRD,
				"version":  rolloutVersion,
				"resource": "rollouts",
			}).Errorf("Error checking if Argo Rollout CRD exists: %v", err)
			// Continue without rollout monitoring
		} else if flag {
			logrus.WithFields(logrus.Fields{
				"pkg":      "kubewatch",
				"crd":      rolloutCRD,
				"version":  rolloutVersion,
				"resource": "rollouts",
			}).Info("Starting watcher for Argo Rollout CRD")
			
			// Define the GVR (Group-Version-Resource) for the Argo Rollout CRD
			rolloutGVR := schema.GroupVersionResource{
				Group:    "argoproj.io",
				Version:  "v1alpha1",
				Resource: "rollouts",
			}

			informer := cache.NewSharedIndexInformer(&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					result, err := dynamicClient.Resource(rolloutGVR).Namespace(conf.Namespace).List(context.Background(), options)
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"pkg":      "kubewatch",
							"crd":      rolloutCRD,
							"version":  rolloutVersion,
							"resource": "rollouts",
						}).Errorf("Failed to list Argo Rollout resources: %v", err)
					}
					return result, err
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					result, err := dynamicClient.Resource(rolloutGVR).Namespace(conf.Namespace).Watch(context.Background(), options)
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"pkg":      "kubewatch",
							"crd":      rolloutCRD,
							"version":  rolloutVersion,
							"resource": "rollouts",
						}).Errorf("Failed to watch Argo Rollout resources: %v", err)
					}
					return result, err
				},
			},
				&unstructured.Unstructured{},
				0, //Skip resync
				cache.Indexers{},
			)

			c := newResourceController(kubeClient, eventHandler, informer, "Rollout", rolloutGVR.Version, kubewatchEventsMetrics, opts)
			stopCh := make(chan struct{})
			defer close(stopCh)

			go c.Run(stopCh)
		} else {
			logrus.WithFields(logrus.Fields{
				"pkg":      "kubewatch",
				"crd":      rolloutCRD,
				"version":  rolloutVersion,
				"resource": "rollouts",
			}).Warnf("Argo Rollout CRD %s (version %s) not found in cluster, skipping rollout monitoring", rolloutCRD, rolloutVersion)
		}
	}

	for _, crd := range conf.CustomResources {
		crdName := fmt.Sprintf("%s.%s", crd.Resource, crd.Group)
		crdVersion := crd.Version
		
		logrus.WithFields(logrus.Fields{
			"pkg":      "kubewatch",
			"crd":      crdName,
			"version":  crdVersion,
			"resource": crd.Resource,
		}).Info("Checking CRD availability")
		
		flag, err := utils.CheckCRDExists(dynamicClient, crdName, crdVersion)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"pkg":      "kubewatch",
				"crd":      crdName,
				"version":  crdVersion,
				"resource": crd.Resource,
			}).Errorf("Error checking if CRD exists: %v", err)
			continue
		}
		if !flag {
			logrus.WithFields(logrus.Fields{
				"pkg":      "kubewatch",
				"crd":      crdName,
				"version":  crdVersion,
				"resource": crd.Resource,
			}).Warnf("CRD %s (version %s) not found in cluster, skipping", crdName, crdVersion)
			continue
		}
		
		logrus.WithFields(logrus.Fields{
			"pkg":      "kubewatch",
			"crd":      crdName,
			"version":  crdVersion,
			"resource": crd.Resource,
		}).Info("Starting watcher for CRD")
		
		informer := cache.NewSharedIndexInformer(
			&cache.ListWatch{
				ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
					result, err := dynamicClient.Resource(schema.GroupVersionResource{
						Group:    crd.Group,
						Version:  crd.Version,
						Resource: crd.Resource,
					}).List(context.Background(), options)
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"pkg":      "kubewatch",
							"crd":      crdName,
							"version":  crdVersion,
							"resource": crd.Resource,
						}).Errorf("Failed to list CRD resources: %v", err)
					}
					return result, err
				},
				WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
					result, err := dynamicClient.Resource(schema.GroupVersionResource{
						Group:    crd.Group,
						Version:  crd.Version,
						Resource: crd.Resource,
					}).Watch(context.Background(), options)
					if err != nil {
						logrus.WithFields(logrus.Fields{
							"pkg":      "kubewatch",
							"crd":      crdName,
							"version":  crdVersion,
							"resource": crd.Resource,
						}).Errorf("Failed to watch CRD resources: %v", err)
					}
					return result, err
				},
			},
			&unstructured.Unstructured{},
			0, //Skip resync
			cache.Indexers{},
		)

		c := newResourceController(kubeClient, eventHandler, informer, fmt.Sprintf("%s/%s", crd.Resource, crd.Group), fmt.Sprintf("%s/%s", crd.Group, crd.Version), kubewatchEventsMetrics, opts)
		stopCh := make(chan struct{})
		defer close(stopCh)
		go c.Run(stopCh)
	}

	// Start workload scheduler if enabled
	if conf.Sync.Enabled && conf.Sync.WorkloadSnapshot.Enabled {
		// Apply defaults to sync config
		conf.SetSyncDefaults()
		
		// Validate sync configuration
		if err := scheduler.ValidateConfig(conf); err != nil {
			logrus.WithError(err).Error("Invalid sync configuration")
		} else {
			// Create workload scheduler
			workloadScheduler := scheduler.NewWorkloadScheduler(kubeClient, conf, eventHandler)
			if err := workloadScheduler.Start(); err != nil {
				logrus.WithError(err).Error("Failed to start workload scheduler")
			} else {
				logrus.Info("Workload scheduler started successfully")
				defer workloadScheduler.Stop()
			}
		}
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	signal.Notify(sigterm, syscall.SIGINT)
	<-sigterm
}

func newResourceController(client kubernetes.Interface, eventHandler handlers.Handler, informer cache.SharedIndexInformer, resourceType string, apiVersion string, kubewatchEventsMetrics *prometheus.CounterVec, opts queueOptions) *Controller {
	logger := logrus.WithField("pkg", "kubewatch-"+resourceType)
	// managedFields is server-side-apply bookkeeping, often 40% of an
	// object's JSON, and no receiver reads it. Strip it before the object
	// enters the informer cache so neither the cache nor the payloads carry it.
	if err := informer.SetTransform(stripManagedFields); err != nil {
		logger.Warnf("cannot strip managedFields from cached objects: %v", err)
	}
	queue := workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[string]{
		Name:            resourceType,
		MetricsProvider: queueMetricsProvider{},
	})
	pending := newPendingEvents(opts.maxPending, func(n int) {
		queueDropped.WithLabelValues(resourceType).Add(float64(n))
	})
	workers := opts.workers
	if workers < 1 {
		workers = 1
	}

	enqueue := func(key, eventType string, obj, oldObj interface{}) {
		ev := queuedEvent{eventType: eventType}
		ev.obj, _ = obj.(runtime.Object)
		if obj != nil && ev.obj == nil {
			logger.Errorf("cannot convert to runtime.Object for %s on %v", eventType, obj)
		}
		if oldObj != nil {
			var ok bool
			if ev.oldObj, ok = oldObj.(runtime.Object); !ok {
				logger.Errorf("cannot convert old to runtime.Object for %s on %v", eventType, oldObj)
			}
		}
		logger.Debugf("Queueing %s of %v: %s", eventType, resourceType, key)
		pending.add(key, ev)
		queue.Add(key)
		kubewatchEventsMetrics.WithLabelValues(resourceType, eventType).Inc()
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Errorf("cannot key added object: %v", err)
				return
			}
			enqueue(key, "create", obj, nil)
		},
		UpdateFunc: func(old, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(old)
			if err != nil {
				logger.Errorf("cannot key updated object: %v", err)
				return
			}
			enqueue(key, "update", new, old)
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Errorf("cannot key deleted object: %v", err)
				return
			}
			// A delete missed while the watch was down arrives as a
			// tombstone wrapping the last known state.
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			enqueue(key, "delete", obj, nil)
		},
	})

	return &Controller{
		logger:       logger,
		clientset:    client,
		informer:     informer,
		queue:        queue,
		pending:      pending,
		eventHandler: eventHandler,
		resourceType: resourceType,
		apiVersion:   apiVersion,
		workers:      workers,
	}
}

// stripManagedFields is the informer transform for every watched type.
// Objects the transform cannot read metadata from (tombstones) pass through.
func stripManagedFields(obj interface{}) (interface{}, error) {
	if accessor, err := meta.Accessor(obj); err == nil {
		accessor.SetManagedFields(nil)
	}
	return obj, nil
}

// Run starts the kubewatch controller
func (c *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting kubewatch controller")
	serverStartTime = time.Now().Local()

	go c.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	c.logger.Infof("Kubewatch controller synced and ready (%d workers)", c.workers)

	// The workqueue never hands one key to two workers at once, so events
	// for the same object are still sent in order.
	for i := 0; i < c.workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	<-stopCh
}

// HasSynced is required for the cache.Controller interface.
func (c *Controller) HasSynced() bool {
	return c.informer.HasSynced()
}

// LastSyncResourceVersion is required for the cache.Controller interface.
func (c *Controller) LastSyncResourceVersion() string {
	return c.informer.LastSyncResourceVersion()
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
		// continue looping
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	// Handlers report delivery failures themselves and processItem only
	// fails on a store read that cannot fail, so nothing is retried.
	for _, ev := range c.pending.take(key) {
		err := c.processItem(Event{
			key:          key,
			eventType:    ev.eventType,
			resourceType: c.resourceType,
			apiVersion:   c.apiVersion,
			obj:          ev.obj,
			oldObj:       ev.oldObj,
		})
		if err != nil {
			c.logger.Errorf("Error processing %s: %v", key, err)
			utilruntime.HandleError(err)
		}
	}
	return true
}

/* TODOs
- Enhance event creation using client-side cacheing machanisms - pending
- Enhance the processItem to classify events - done
- Send alerts correspoding to events - done
*/

func (c *Controller) processItem(newEvent Event) error {
	// NOTE that obj will be nil on deletes!
	obj, _, err := c.informer.GetIndexer().GetByKey(newEvent.key)

	if err != nil {
		return fmt.Errorf("Error fetching object with key %s from store: %v", newEvent.key, err)
	}
	// get object's metedata
	objectMeta := utils.GetObjectMetaData(obj)

	// hold status type for default critical alerts
	var status string

	// namespace retrived from event key incase namespace value is empty
	if newEvent.namespace == "" && strings.Contains(newEvent.key, "/") {
		substring := strings.Split(newEvent.key, "/")
		newEvent.namespace = substring[0]
		newEvent.key = substring[1]
	} else {
		newEvent.namespace = objectMeta.Namespace
	}

	// process events based on its type
	switch newEvent.eventType {
	case "create":
		// compare CreationTimestamp and serverStartTime and alert only on latest events
		// Could be Replaced by using Delta or DeltaFIFO
		if objectMeta.CreationTimestamp.Sub(serverStartTime).Seconds() > 0 {
			switch newEvent.resourceType {
			case "NodeNotReady":
				status = "Danger"
			case "NodeReady":
				status = "Normal"
			case "NodeRebooted":
				status = "Danger"
			case "Backoff":
				status = "Danger"
			default:
				status = "Normal"
			}
			kbEvent := event.Event{
				Name:       newEvent.key,
				Namespace:  newEvent.namespace,
				Kind:       newEvent.resourceType,
				ApiVersion: newEvent.apiVersion,
				Status:     status,
				Reason:     "Created",
				Obj:        newEvent.obj,
			}
			c.eventHandler.Handle(kbEvent)
			return nil
		}
	case "update":
		/* TODOs
		- enahace update event processing in such a way that, it send alerts about what got changed.
		*/
		switch newEvent.resourceType {
		case "Backoff":
			status = "Danger"
		default:
			status = "Warning"
		}
		kbEvent := event.Event{
			Name:       newEvent.key,
			Namespace:  newEvent.namespace,
			Kind:       newEvent.resourceType,
			ApiVersion: newEvent.apiVersion,
			Status:     status,
			Reason:     "Updated",
			Obj:        newEvent.obj,
			OldObj:     newEvent.oldObj,
		}
		c.eventHandler.Handle(kbEvent)
		return nil
	case "delete":
		kbEvent := event.Event{
			Name:       newEvent.key,
			Namespace:  newEvent.namespace,
			Kind:       newEvent.resourceType,
			ApiVersion: newEvent.apiVersion,
			Status:     "Danger",
			Reason:     "Deleted",
			Obj:        newEvent.obj,
		}
		c.eventHandler.Handle(kbEvent)
		return nil
	}
	return nil
}
