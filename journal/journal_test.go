package journal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	th "github.com/filecoin-project/go-filecoin/testhelpers"
	tf "github.com/filecoin-project/go-filecoin/testhelpers/testflags"
)

func TestSimpleInMemoryJournal(t *testing.T) {
	tf.UnitTest(t)

	mj := NewInMemoryJournal(th.NewFakeSystemClock(time.Unix(1234567890, 0)))
	topicJ := mj.Topic("testing")
	topicJ.Write("event1", "foo", "bar")

	memoryWriter, ok := topicJ.(*MemoryWriter)
	assert.True(t, ok)
	assert.Equal(t, 1, len(memoryWriter.entries))
	assert.Equal(t, "testing", memoryWriter.topic)

	topicJ.Write("event2", "number", 42)
	assert.Equal(t, 2, len(memoryWriter.entries))

	obj := struct {
		Name string
		Arg  int
	}{"bob",
		42,
	}
	topicJ.Write("event3", "object", obj, "name", "bob", "age", 42)
	assert.Equal(t, 3, len(memoryWriter.entries))

	assert.Equal(t, "bar", memoryWriter.entries[0].kvs["foo"])
	assert.Equal(t, 42, memoryWriter.entries[1].kvs["number"])
	assert.Equal(t, obj, memoryWriter.entries[2].kvs["object"])
	assert.Equal(t, "bob", memoryWriter.entries[2].kvs["name"])
	assert.Equal(t, 42, memoryWriter.entries[2].kvs["age"])
}
