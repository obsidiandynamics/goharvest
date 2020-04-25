package goharvest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue_concurrencyOf1(t *testing.T) {
	enqueued := make(chan OutboxRecord)
	b := newConcurrentBattery(1, 0, func(records chan OutboxRecord) {
		for rec := range records {
			enqueued <- rec
		}
	})
	defer b.shutdown()

	rec := OutboxRecord{}
	assert.True(t, b.enqueue(rec))
	assert.Equal(t, rec, <-enqueued)
}

func TestEnqueue_concurrencyOf2(t *testing.T) {
	enqueued := make(chan OutboxRecord)
	b := newConcurrentBattery(2, 0, func(records chan OutboxRecord) {
		for rec := range records {
			enqueued <- rec
		}
	})
	defer b.shutdown()

	rec := OutboxRecord{}
	assert.True(t, b.enqueue(rec))
	assert.Equal(t, rec, <-enqueued)
}

func TestEnqueue_afterDone(t *testing.T) {
	b := newConcurrentBattery(2, 0, func(records chan OutboxRecord) {})
	b.await()

	assert.False(t, b.enqueue(OutboxRecord{}))
	b.stop()
}
