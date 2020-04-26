package goharvest

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// KafkaHeader is a key-value tuple representing a single header entry.
type KafkaHeader struct {
	Key   string
	Value string
}

// String obtains a textual representation of a KafkaHeader.
func (h KafkaHeader) String() string {
	return h.Key + ":" + h.Value
}

// KafkaHeaders is a slice of KafkaHeader tuples.
type KafkaHeaders []KafkaHeader

// OutboxRecord depicts a single entry in the outbox table. It can be used for both reading and writing operations.
type OutboxRecord struct {
	ID           int64
	CreateTime   time.Time
	KafkaTopic   string
	KafkaKey     string
	KafkaValue   *string
	KafkaHeaders KafkaHeaders
	LeaderID     *uuid.UUID
}

// StringPtr is a convenience function that returns a pointer to the given str argument, for use with setting OutboxRecord.Value.
func StringPtr(str string) *string {
	return &str
}

// String provides a textual representation of an OutboxRecord.
func (rec OutboxRecord) String() string {
	return fmt.Sprint("OutboxRecord[ID=", rec.ID,
		", CreateTime=", rec.CreateTime,
		", KafkaTopic=", rec.KafkaTopic,
		", KafkaKey=", rec.KafkaKey,
		", KafkaValue=", rec.KafkaValue,
		", KafkaHeaders=", rec.KafkaHeaders,
		", LeaderID=", rec.LeaderID, "]")
}

// DatabaseBinding is an abstraction over the data access layer, allowing goharvest to use arbitrary database implementations.
type DatabaseBinding interface {
	Mark(leaderID uuid.UUID, limit int) ([]OutboxRecord, error)
	Purge(id int64) (bool, error)
	Reset(id int64) (bool, error)
	Dispose()
}

// DatabaseBindingProvider is a factory for creating instances of a DatabaseBinding.
type DatabaseBindingProvider func(dataSource string, outboxTable string) (DatabaseBinding, error)
