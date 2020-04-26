package goharvest

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/google/uuid"

	// init postgres driver
	"github.com/lib/pq"
)

type database struct {
	db        *sql.DB
	markStmt  *sql.Stmt
	purgeStmt *sql.Stmt
	resetStmt *sql.Stmt
}

const markQueryTemplate = `
-- mark query
UPDATE %s
SET leader_id = $1
WHERE id IN (
  SELECT id FROM %s
  WHERE leader_id IS NULL OR leader_id != $1
  ORDER BY id
  LIMIT $2
)
RETURNING id, create_time, kafka_topic, kafka_key, kafka_value, kafka_header_keys, kafka_header_values, leader_id
`

const purgeQueryTemplate = `
-- purge query
DELETE FROM %s
WHERE id = $1
`

const resetQueryTemplate = `
-- reset query
UPDATE %s
SET leader_id = NULL
WHERE id = $1
`

func closeResource(stmt *sql.Stmt) {
	if stmt != nil {
		stmt.Close()
	}
}

func closeResources(stmts ...*sql.Stmt) {
	for _, resource := range stmts {
		closeResource(resource)
	}
}

type databaseProvider func() (*sql.DB, error)

// StandardPostgresBindingProvider returns a DatabaseBindingProvider that connects to a real Postgres database.
func StandardPostgresBindingProvider() DatabaseBindingProvider {
	return NewPostgresBinding
}

// NewPostgresBinding creates a Postgres binding for the given dataSource and outboxTable args.
func NewPostgresBinding(dataSource string, outboxTable string) (DatabaseBinding, error) {
	return newPostgresBinding(func() (*sql.DB, error) {
		return sql.Open("postgres", dataSource)
	}, outboxTable)
}

func newPostgresBinding(dbProvider databaseProvider, outboxTable string) (DatabaseBinding, error) {
	success := false
	var db *sql.DB
	var markStmt, purgeStmt, resetStmt *sql.Stmt
	defer func() {
		if !success {
			if db != nil {
				db.Close()
			}
			closeResources(markStmt, purgeStmt, resetStmt)
		}
	}()

	db, err := dbProvider()
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	markStmt, err = db.Prepare(fmt.Sprintf(markQueryTemplate, outboxTable, outboxTable))
	if err != nil {
		return nil, err
	}

	purgeStmt, err = db.Prepare(fmt.Sprintf(purgeQueryTemplate, outboxTable))
	if err != nil {
		return nil, err
	}

	resetStmt, err = db.Prepare(fmt.Sprintf(resetQueryTemplate, outboxTable))
	if err != nil {
		return nil, err
	}

	success = true
	return &database{
		db:        db,
		markStmt:  markStmt,
		purgeStmt: purgeStmt,
		resetStmt: resetStmt,
	}, nil
}

func (db *database) Mark(leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
	rows, err := db.markStmt.Query(leaderID, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	records := make([]OutboxRecord, 0, limit)
	for rows.Next() {
		record := OutboxRecord{}
		var keys []string
		var values []string
		err := rows.Scan(
			&record.ID,
			&record.CreateTime,
			&record.KafkaTopic,
			&record.KafkaKey,
			&record.KafkaValue,
			pq.Array(&keys),
			pq.Array(&values),
			&record.LeaderID,
		)
		if err != nil {
			return nil, err
		}
		numKeys := len(keys)
		if len(keys) != len(values) {
			return nil, fmt.Errorf("unequal number of header keys (%d) and values (%d)", numKeys, len(values))
		}

		record.KafkaHeaders = make(KafkaHeaders, numKeys)
		for i := 0; i < numKeys; i++ {
			record.KafkaHeaders[i] = KafkaHeader{keys[i], values[i]}
		}
		records = append(records, record)
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].ID < records[j].ID
	})

	return records, nil
}

func (db *database) Purge(id int64) (bool, error) {
	res, err := db.purgeStmt.Exec(id)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		return false, nil
	}
	return true, err
}

func (db *database) Reset(id int64) (bool, error) {
	res, err := db.resetStmt.Exec(id)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	if affected != 1 {
		return false, nil
	}
	return true, err
}

func (db *database) Dispose() {
	db.db.Close()
	closeResources(db.markStmt, db.purgeStmt, db.resetStmt)
}
