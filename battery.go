package goharvest

import (
	"hash/fnv"
)

type cell struct {
	records chan OutboxRecord
	done    chan int
}

func (c cell) stop() {
	close(c.records)
}

func (c cell) await() {
	<-c.done
}

func (c cell) enqueue(rec OutboxRecord) bool {
	select {
	case <-c.done:
		return false
	case c.records <- rec:
		return true
	}
}

type cellHandler func(records chan OutboxRecord)

func newCell(buffer int, handler cellHandler) cell {
	c := cell{
		records: make(chan OutboxRecord),
		done:    make(chan int),
	}
	go func() {
		defer close(c.done)
		handler(c.records)
	}()
	return c
}

type battery interface {
	stop()
	await()
	shutdown()
	enqueue(rec OutboxRecord) bool
}

type concurrentBattery []cell

func (b *concurrentBattery) stop() {
	for _, c := range *b {
		c.stop()
	}
}

func (b *concurrentBattery) await() {
	for _, c := range *b {
		c.await()
	}
}

func (b *concurrentBattery) shutdown() {
	b.stop()
	b.await()
}

func (b *concurrentBattery) enqueue(rec OutboxRecord) bool {
	if length := len(*b); length > 1 {
		return (*b)[hash(rec.KafkaKey)%uint32(length)].enqueue(rec)
	}
	return (*b)[0].enqueue(rec)
}

func newConcurrentBattery(concurrency int, buffer int, handler cellHandler) *concurrentBattery {
	b := make(concurrentBattery, concurrency)
	for i := 0; i < concurrency; i++ {
		b[i] = newCell(buffer, handler)
	}
	return &b
}

func hash(str string) uint32 {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(str))
	return algorithm.Sum32()
}
