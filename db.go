package goharvest

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type KafkaHeader struct {
	Key   string
	Value string
}

func (h KafkaHeader) String() string {
	return h.Key + ":" + h.Value
}

type KafkaHeaders []KafkaHeader

type OutboxRecord struct {
	ID           int64
	CreateTime   time.Time
	KafkaTopic   string
	KafkaKey     string
	KafkaValue   *string
	KafkaHeaders KafkaHeaders
	LeaderID     *uuid.UUID
}

func StringPtr(str string) *string {
	return &str
}

func (rec OutboxRecord) String() string {
	return fmt.Sprint("OutboxRecord[ID=", rec.ID,
		", CreateTime=", rec.CreateTime,
		", KafkaTopic=", rec.KafkaTopic,
		", KafkaKey=", rec.KafkaKey,
		", KafkaValue=", rec.KafkaValue,
		", KafkaHeaders=", rec.KafkaHeaders,
		", LeaderID=", rec.LeaderID, "]")
}

type DatabaseBinding interface {
	Mark(leaderID uuid.UUID, limit int) ([]OutboxRecord, error)
	Purge(id int64) (bool, error)
	Reset(id int64) (bool, error)
	Dispose()
}

type DatabaseBindingProvider func(dataSource string, outboxTable string) (DatabaseBinding, error)
