package engine

import "time"

// EventType names an engine event published to Options.OnEvent. The admin
// server turns these into the Server-Sent-Events stream that drives the LSM
// Inspector UI.
type EventType string

const (
	EventMemtableSealed  EventType = "memtable_sealed"
	EventFlushStart      EventType = "flush_start"
	EventFlushEnd        EventType = "flush_end"
	EventFileCreated     EventType = "file_created"
	EventFileDeleted     EventType = "file_deleted"
	EventCompactionStart EventType = "compaction_start"
	EventCompactionEnd   EventType = "compaction_end"
	EventWriteStall      EventType = "write_stall"
	EventWALRotated      EventType = "wal_rotated"
	EventRecovery        EventType = "recovery"
)

// Event is one engine occurrence. Fields not relevant to a type are zero.
type Event struct {
	Type   EventType `json:"type"`
	Time   time.Time `json:"time"`
	File   uint64    `json:"file,omitempty"`
	Level  int       `json:"level,omitempty"`
	Bytes  int64     `json:"bytes,omitempty"`
	Count  int64     `json:"count,omitempty"`
	Detail string    `json:"detail,omitempty"`
	// Inputs/Outputs list file numbers for compactions.
	Inputs  []uint64 `json:"inputs,omitempty"`
	Outputs []uint64 `json:"outputs,omitempty"`
	Nanos   int64    `json:"nanos,omitempty"` // duration, for *_end events
}

func (d *DB) emit(e Event) {
	if d.opts.OnEvent != nil {
		e.Time = time.Now()
		d.opts.OnEvent(e)
	}
}
