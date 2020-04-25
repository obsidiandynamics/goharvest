package stasher

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/obsidiandynamics/goharvest"
)

type Stasher interface {
	Stash(tx *sql.Tx, rec goharvest.OutboxRecord) error
	Prepare(tx *sql.Tx) (PreStash, error)
}

type stasher struct {
	query string
}

func NewStasher(outboxTable string) Stasher {
	return &stasher{fmt.Sprintf(insertQueryTemplate, outboxTable)}
}

const insertQueryTemplate = `
-- insert query
INSERT INTO %s (create_time, kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values)
VALUES (NOW(), $1, $2, $3, $4, $5)
`

type PreStash struct {
	stmt *sql.Stmt
}

func (s *stasher) Prepare(tx *sql.Tx) (PreStash, error) {
	stmt, err := tx.Prepare(s.query)
	if err != nil {
		return PreStash{}, err
	}
	return PreStash{stmt}, nil
}

func (p PreStash) Stash(rec goharvest.OutboxRecord) error {
	var headerKeys, headerValues []string
	if numHeaders := len(rec.KafkaHeaders); numHeaders > 0 {
		headerKeys = make([]string, numHeaders)
		headerValues = make([]string, numHeaders)
		for i, header := range rec.KafkaHeaders {
			headerKeys[i], headerValues[i] = header.Key, header.Value
		}
	} else {
		headerKeys, headerValues = []string{}, []string{}
	}
	_, err := p.stmt.Exec(rec.KafkaTopic, rec.KafkaKey, rec.KafkaValue, pq.Array(headerKeys), pq.Array(headerValues))
	return err
}

func (s *stasher) Stash(tx *sql.Tx, rec goharvest.OutboxRecord) error {
	_, err := tx.Exec(s.query, rec.KafkaTopic, rec.KafkaKey, rec.KafkaValue)
	return err
}
