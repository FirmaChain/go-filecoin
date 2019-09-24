package journal

import (
	"fmt"
	"strings"
	"time"

	"github.com/filecoin-project/go-filecoin/clock"
)

// Writer defines an interface for recording events and their metadata
type Writer interface {
	// Write records an operation and its metadata to a Journal accepting variadic key-value
	// pairs.
	Write(event string, kvs ...interface{})
}

// Journal defines an interface for creating Journals with a topic.
type Journal interface {
	// Topic returns a Writer that records events for a topic.
	Topic(topic string) Writer
}

// NewInMemoryJournal returns a journal backed by an in-memory map.
func NewInMemoryJournal(clk clock.Clock) Journal {
	return &MemoryJournal{
		clock:  clk,
		topics: make(map[string]struct{}),
	}
}

// MemoryJournal represents a journal held in memory.
type MemoryJournal struct {
	clock   clock.Clock
	topics  map[string]struct{}
	writers []*MemoryWriter
}

// Topic returns a Writer with the provided `topic`.
func (mj *MemoryJournal) Topic(topic string) Writer {
	if _, ok := mj.topics[topic]; ok {
		// this means we have already made a journal here
		// fail?
		panic("duplicate topics developer error")
	}
	mr := &MemoryWriter{
		clock: mj.clock,
		topic: topic,
	}
	mj.writers = append(mj.writers, mr)
	return mr
}

// String returns all Journal topics and their entries as a string.
func (mj *MemoryJournal) String() string {
	var sb strings.Builder
	for _, writer := range mj.writers {
		sb.WriteString(fmt.Sprintf("topic: %s", writer.topic))
		sb.WriteRune('\n')
		for _, ent := range writer.entries {
			sb.WriteString(fmt.Sprintf("\t%s - %s: %v", ent.time.String(), ent.event, ent.kvs))
			sb.WriteRune('\n')
		}
	}
	return sb.String()
}

type entry struct {
	time  time.Time
	event string
	kvs   map[string]interface{}
}

// MemoryWriter writes journal entires in memory.
type MemoryWriter struct {
	clock   clock.Clock
	topic   string
	entries []entry
}

// Write records an operation and its metadata to a Journal accepting variadic key-value
// pairs.
func (mw *MemoryWriter) Write(event string, kvs ...interface{}) {
	var ent entry
	ent.time = mw.clock.Now()
	ent.event = event
	extracted, err := extractKeyValues(kvs...)
	if err != nil {
		panic(err)
	}
	ent.kvs = extracted
	mw.entries = append(mw.entries, ent)
}

// extractKeyValues extracts the keys and values from `kvs` to a map. An error is returned if there
// is not a 1 to 1 mapping between keys and values or if a key is not of type string.
func extractKeyValues(kvs ...interface{}) (map[string]interface{}, error) {
	// no op
	if len(kvs) == 0 {
		return nil, nil
	}
	// edge case
	if len(kvs) == 1 {
		return nil, fmt.Errorf("dangling field %v", kvs[0])
	}
	out := make(map[string]interface{}, len(kvs)/2)
	for i := 0; i < len(kvs); {
		if i == len(kvs)-1 {
			return nil, fmt.Errorf("dangling field %s", kvs[i])
		}
		key, value := kvs[i], kvs[i+1]
		keyStr, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("key is not of type string")
		}
		out[keyStr] = value
		i += 2
	}
	return out, nil
}
