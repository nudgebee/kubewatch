package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitnami-labs/kubewatch/config"
	"github.com/bitnami-labs/kubewatch/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
	api_v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

func pod(name, rv string) *api_v1.Pod {
	return &api_v1.Pod{ObjectMeta: meta_v1.ObjectMeta{Name: name, Namespace: "ns", ResourceVersion: rv}}
}

func rv(o runtime.Object) string {
	if o == nil {
		return ""
	}
	return o.(*api_v1.Pod).ResourceVersion
}

func TestPendingEvents_MergesConsecutiveUpdates(t *testing.T) {
	p := newPendingEvents(0, nil)
	p.add("ns/a", queuedEvent{eventType: "create", obj: pod("a", "1")})
	p.add("ns/a", queuedEvent{eventType: "update", obj: pod("a", "2"), oldObj: pod("a", "1")})
	p.add("ns/a", queuedEvent{eventType: "update", obj: pod("a", "3"), oldObj: pod("a", "2")})
	p.add("ns/a", queuedEvent{eventType: "update", obj: pod("a", "4"), oldObj: pod("a", "3")})

	got := p.take("ns/a")
	if len(got) != 2 {
		t.Fatalf("got %d events; want create + one merged update", len(got))
	}
	if got[0].eventType != "create" || rv(got[0].obj) != "1" {
		t.Errorf("first event = %s rv %s; want the create", got[0].eventType, rv(got[0].obj))
	}
	if got[1].eventType != "update" || rv(got[1].oldObj) != "1" || rv(got[1].obj) != "4" {
		t.Errorf("merged update old=%s new=%s; want old=1 (before the run) new=4 (latest)",
			rv(got[1].oldObj), rv(got[1].obj))
	}
	if p.take("ns/a") != nil {
		t.Error("take returned events twice")
	}
}

func TestPendingEvents_DeleteIsNotMerged(t *testing.T) {
	p := newPendingEvents(0, nil)
	p.add("ns/a", queuedEvent{eventType: "update", obj: pod("a", "2"), oldObj: pod("a", "1")})
	p.add("ns/a", queuedEvent{eventType: "delete", obj: pod("a", "2")})
	p.add("ns/a", queuedEvent{eventType: "create", obj: pod("a", "5")})
	got := p.take("ns/a")
	if len(got) != 3 || got[0].eventType != "update" || got[1].eventType != "delete" || got[2].eventType != "create" {
		t.Fatalf("got %v; want update, delete, create in order", got)
	}
}

func TestPendingEvents_EvictsLongestWaitingOverLimit(t *testing.T) {
	dropped := 0
	p := newPendingEvents(2, func(n int) { dropped += n })
	p.add("ns/a", queuedEvent{eventType: "create", obj: pod("a", "1")})
	p.add("ns/a", queuedEvent{eventType: "delete", obj: pod("a", "1")})
	p.add("ns/b", queuedEvent{eventType: "create", obj: pod("b", "1")})
	p.add("ns/b", queuedEvent{eventType: "update", obj: pod("b", "2"), oldObj: pod("b", "1")}) // existing key: no eviction
	p.add("ns/c", queuedEvent{eventType: "create", obj: pod("c", "1")})                        // evicts a

	if dropped != 2 {
		t.Errorf("dropped = %d; want 2 (a's create and delete)", dropped)
	}
	if p.len() != 2 {
		t.Errorf("pending objects = %d; want 2", p.len())
	}
	if p.take("ns/a") != nil {
		t.Error("evicted object still has events")
	}
	if len(p.take("ns/b")) != 2 || len(p.take("ns/c")) != 1 {
		t.Error("objects under the limit lost events")
	}
}

func TestPendingEvents_PerObjectCap(t *testing.T) {
	dropped := 0
	p := newPendingEvents(0, func(n int) { dropped += n })
	for i := 0; i < maxEventsPerKey+3; i++ {
		typ := "create"
		if i%2 == 1 {
			typ = "delete"
		}
		p.add("ns/a", queuedEvent{eventType: typ, obj: pod("a", fmt.Sprint(i))})
	}
	got := p.take("ns/a")
	if len(got) != maxEventsPerKey || dropped != 3 {
		t.Fatalf("kept %d dropped %d; want %d kept, 3 dropped", len(got), dropped, maxEventsPerKey)
	}
	if rv(got[0].obj) != "3" || rv(got[len(got)-1].obj) != fmt.Sprint(maxEventsPerKey+2) {
		t.Errorf("kept rv %s..%s; want the newest events", rv(got[0].obj), rv(got[len(got)-1].obj))
	}
}

// gatedHandler blocks every Handle until released, recording what it got.
type gatedHandler struct {
	gate chan struct{}
	mu   sync.Mutex
	got  []event.Event
}

func (h *gatedHandler) Init(*config.Config) error { return nil }
func (h *gatedHandler) Handle(e event.Event) {
	<-h.gate
	h.mu.Lock()
	h.got = append(h.got, e)
	h.mu.Unlock()
}
func (h *gatedHandler) events() []event.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]event.Event(nil), h.got...)
}

// While the receiver is stalled, a stream of updates to one Pod must hold one
// queue entry, not one per update, and must still arrive as a single update
// spanning the whole run with managedFields stripped.
func TestController_StalledReceiverQueuesPerObject(t *testing.T) {
	cs := fake.NewSimpleClientset()
	inf := informers.NewSharedInformerFactory(cs, 0).Core().V1().Pods().Informer()
	h := &gatedHandler{gate: make(chan struct{})}
	metrics := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t"}, []string{"a", "b"})
	c := newResourceController(cs, h, inf, "Pod", V1, metrics, queueOptions{workers: 2, maxPending: 100})

	stop := make(chan struct{})
	defer close(stop)
	go c.Run(stop)
	waitUntil(t, c.HasSynced)

	ctx := context.Background()
	p := pod("web", "")
	p.ManagedFields = []meta_v1.ManagedFieldsEntry{{Manager: "kubectl"}}
	// Creates are only sent for objects newer than the controller start.
	p.CreationTimestamp = meta_v1.NewTime(time.Now().Add(time.Hour))
	if _, err := cs.CoreV1().Pods("ns").Create(ctx, p, meta_v1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// The first event is taken by a worker, which blocks in Handle.
	waitUntil(t, func() bool { return c.pending.len() == 0 && c.queue.Len() == 0 })

	for i := 0; i < 50; i++ {
		p.Labels = map[string]string{"gen": fmt.Sprint(i)}
		if _, err := cs.CoreV1().Pods("ns").Update(ctx, p, meta_v1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, func() bool { return c.pending.len() == 1 })
	if n := c.queue.Len(); n > 1 {
		t.Fatalf("queue holds %d entries for one object; want at most 1", n)
	}

	close(h.gate)
	waitUntil(t, func() bool { return len(h.events()) == 2 })
	got := h.events()
	if got[0].Reason != "Created" || got[1].Reason != "Updated" {
		t.Fatalf("got %s, %s; want Created then Updated", got[0].Reason, got[1].Reason)
	}
	oldPod, newPod := got[1].OldObj.(*api_v1.Pod), got[1].Obj.(*api_v1.Pod)
	// The old object is the state before the first update of the run.
	if _, had := oldPod.Labels["gen"]; had || newPod.Labels["gen"] != "49" {
		t.Errorf("merged update spans gen %q -> %q; want unset -> 49", oldPod.Labels["gen"], newPod.Labels["gen"])
	}
	if len(newPod.ManagedFields) != 0 {
		t.Error("managedFields not stripped from the sent object")
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
