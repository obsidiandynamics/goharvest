// Package stasher is a helper for inserting records into an outbox table within transaction scope.
package stasher

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/obsidiandynamics/goharvest"
)

// Stasher writes records into the outbox table.
type Stasher interface {
	Stash(tx *sql.Tx, rec goharvest.OutboxRecord) error
	Prepare(tx *sql.Tx) (PreStash, error)
}

type stasher struct {
	query string
}

// New creates a new Stasher instance for the given outboxTable.
func New(outboxTable string) Stasher {
	return &stasher{fmt.Sprintf(insertQueryTemplate, outboxTable)}
}

const insertQueryTemplate = `
-- insert query
INSERT INTO %s (create_time, kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values)
VALUES (NOW(), $1, $2, $3, $4, $5)
`

// PreStash houses a prepared statement bound to the scope of a single transaction.
type PreStash struct {
	stmt *sql.Stmt
}

// Prepare a statement for stashing records, where the latter is expected to be invoked multiple times from
// a given transaction.
func (s *stasher) Prepare(tx *sql.Tx) (PreStash, error) {
	stmt, err := tx.Prepare(s.query)
	return PreStash{stmt}, err
}

// Stash one record using the prepared statement.
func (p PreStash) Stash(rec goharvest.OutboxRecord) error {
	headerKeys, headerValues := makeHeaders(rec)
	_, err := p.stmt.Exec(rec.KafkaTopic, rec.KafkaKey, rec.KafkaValue, pq.Array(headerKeys), pq.Array(headerValues))
	return err
}

func makeHeaders(rec goharvest.OutboxRecord) ([]string, []string) {
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
	return headerKeys, headerValues
}

// Stash one record within the given transaction scope.
func (s *stasher) Stash(tx *sql.Tx, rec goharvest.OutboxRecord) error {
	headerKeys, headerValues := makeHeaders(rec)
	_, err := tx.Exec(s.query, rec.KafkaTopic, rec.KafkaKey, rec.KafkaValue, pq.Array(headerKeys), pq.Array(headerValues))
	return err
}
