package journal

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

func NewNoopJournal() Journal { return &NoopJournal{} }

type NoopJournal struct{}

func (nj *NoopJournal) Topic(topic string) Writer { return &NoopWriter{} }

type NoopWriter struct{}

func (nw *NoopWriter) Write(event string, kvs ...interface{}) {}
