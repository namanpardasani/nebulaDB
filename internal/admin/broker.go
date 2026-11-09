// Package admin is NebulaDB's HTTP admin server (separate port, loopback by
// default): read-only JSON about engine state, a Prometheus /metrics endpoint,
// and a Server-Sent-Events stream of engine events that drives the LSM
// Inspector UI. A tiny whitelisted command proxy and a few dev-only endpoints
// are included for the Inspector's "try it" and "crash it" panels.
package admin

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/naman/nebuladb/internal/engine"
)

// Envelope is one stream message: a monotonically increasing id (0 for
// ephemeral messages that are never replayed) and its JSON body.
type Envelope struct {
	ID   uint64
	Data []byte
}

// Broker fans engine events out to SSE subscribers. Publishing never blocks the
// engine: each subscriber has a bounded buffer and a subscriber that cannot keep
// up is dropped (its HTTP handler ends the stream; browsers reconnect with
// Last-Event-ID and receive the replayed backlog).
type Broker struct {
	mu     sync.Mutex
	ring   []Envelope // last `cap` persisted events, oldest first
	cap    int
	nextID uint64
	subs   map[*Subscriber]struct{}
	// OnSubscribers is called (without the lock) when the number of subscribers
	// changes, so the tick generator can idle while nobody is watching.
	onChange func(n int)
}

// Subscriber receives envelopes on C until Close.
type Subscriber struct {
	C       chan Envelope
	b       *Broker
	dropped bool
}

// NewBroker keeps the last size persisted events for replay.
func NewBroker(size int) *Broker {
	// Event ids must keep increasing ACROSS PROCESS RESTARTS: after a crash the
	// browser reconnects with the Last-Event-ID of the dead process, and the
	// replay filter (id > lastID) would otherwise skip every event of the new
	// one (including its recovery report). Starting from the wall clock in
	// microseconds keeps ids unique, ordered, and below 2^53 so JavaScript can
	// represent them exactly.
	return &Broker{cap: size, subs: map[*Subscriber]struct{}{}, nextID: uint64(time.Now().UnixMicro())}
}

// Publish records and broadcasts an engine event (persisted for replay).
func (b *Broker) Publish(e engine.Event) {
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	b.publish(data, true)
}

// PublishEphemeral broadcasts a message that is not stored (ticks).
func (b *Broker) PublishEphemeral(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	b.publish(data, false)
}

func (b *Broker) publish(data []byte, persist bool) {
	b.mu.Lock()
	env := Envelope{Data: data}
	if persist {
		env.ID = b.nextID
		b.nextID++
		if len(b.ring) >= b.cap {
			copy(b.ring, b.ring[1:])
			b.ring = b.ring[:len(b.ring)-1]
		}
		b.ring = append(b.ring, env)
	}
	for s := range b.subs {
		select {
		case s.C <- env:
		default: // too slow: drop it rather than stall the engine
			s.dropped = true
			delete(b.subs, s)
			close(s.C)
		}
	}
	b.mu.Unlock()
}

// Subscribe registers a subscriber and returns it with the backlog of
// persisted events whose id is greater than lastID (replay for reconnects).
func (b *Broker) Subscribe(lastID uint64) (*Subscriber, []Envelope) {
	b.mu.Lock()
	s := &Subscriber{C: make(chan Envelope, 1024), b: b}
	var backlog []Envelope
	for _, e := range b.ring {
		if e.ID > lastID {
			backlog = append(backlog, e)
		}
	}
	b.subs[s] = struct{}{}
	n := len(b.subs)
	cb := b.onChange
	b.mu.Unlock()
	if cb != nil {
		cb(n)
	}
	return s, backlog
}

// Close unsubscribes.
func (s *Subscriber) Close() {
	b := s.b
	b.mu.Lock()
	if _, ok := b.subs[s]; ok {
		delete(b.subs, s)
		close(s.C)
	}
	n := len(b.subs)
	cb := b.onChange
	b.mu.Unlock()
	if cb != nil {
		cb(n)
	}
}

// Dropped reports whether the broker disconnected this subscriber for being slow.
func (s *Subscriber) Dropped() bool {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	return s.dropped
}

// Subscribers returns the current subscriber count.
func (b *Broker) Subscribers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}
