package controller

import (
	"container/list"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
)

// pendingEvents holds the not-yet-sent informer events of one resource type,
// grouped by object key. The workqueue carries only the key, so it stays
// bounded by the number of objects (the workqueue dedups equal keys) instead
// of growing by one entry per update, each pinning two full objects that the
// informer cache has already replaced.
//
// Consecutive updates to one object are merged while they wait: the merged
// event carries the first update's old object and the latest new object, so
// a receiver diffing old against new still sees every field that changed over
// the span. Updates are only merged while the sender is behind; in steady
// state each event is taken before the next arrives and nothing merges.
//
// maxKeys is the safety valve. Above it, the objects that have waited longest
// lose their pending events, which are counted as drops, instead of the
// process growing until it is OOM-killed.
type pendingEvents struct {
	mu        sync.Mutex
	byKey     map[string]*pendingEntry
	order     *list.List // keys by first-pending time; front waited longest
	maxKeys   int        // <= 0: unbounded
	maxPerKey int
	onDrop    func(n int)
}

type pendingEntry struct {
	events []queuedEvent
	elem   *list.Element
}

type queuedEvent struct {
	eventType string // create, update, delete
	obj       runtime.Object
	oldObj    runtime.Object
}

// maxEventsPerKey caps the events waiting for one object. Merging keeps it at
// one per run of updates; only an object deleted and recreated repeatedly
// while the sender is stalled can reach the cap, and then its oldest events go.
const maxEventsPerKey = 8

func newPendingEvents(maxKeys int, onDrop func(n int)) *pendingEvents {
	if onDrop == nil {
		onDrop = func(int) {}
	}
	return &pendingEvents{
		byKey:     map[string]*pendingEntry{},
		order:     list.New(),
		maxKeys:   maxKeys,
		maxPerKey: maxEventsPerKey,
		onDrop:    onDrop,
	}
}

// add records an event for key. The caller then adds key to the workqueue.
func (p *pendingEvents) add(key string, ev queuedEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e, ok := p.byKey[key]; ok {
		last := &e.events[len(e.events)-1]
		if last.eventType == "update" && ev.eventType == "update" {
			last.obj = ev.obj // keep last.oldObj: the state before the run of updates
			return
		}
		e.events = append(e.events, ev)
		if over := len(e.events) - p.maxPerKey; over > 0 {
			e.events = append(e.events[:0:0], e.events[over:]...)
			p.onDrop(over)
		}
		return
	}

	for p.maxKeys > 0 && len(p.byKey) >= p.maxKeys {
		front := p.order.Front()
		oldest := front.Value.(string)
		p.onDrop(len(p.byKey[oldest].events))
		delete(p.byKey, oldest)
		p.order.Remove(front)
		// The evicted key stays in the workqueue; take returns nothing for it.
	}
	p.byKey[key] = &pendingEntry{
		events: []queuedEvent{ev},
		elem:   p.order.PushBack(key),
	}
}

// take removes and returns the events pending for key, oldest first. Returns
// nil when they were evicted or already taken.
func (p *pendingEvents) take(key string) []queuedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.byKey[key]
	if !ok {
		return nil
	}
	delete(p.byKey, key)
	p.order.Remove(e.elem)
	return e.events
}

// len returns the number of objects with pending events.
func (p *pendingEvents) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byKey)
}
