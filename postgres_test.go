package goharvest

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const outboxTable = "outbox"
const markPrepare = "-- mark query"
const purgePrepare = "-- purge query"
const resetPrepare = "-- reset query"

func pgFixtures() (databaseProvider, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	dbProvider := func() (*sql.DB, error) {
		return db, nil
	}
	return dbProvider, mock
}

func TestErrorInDBProvider(t *testing.T) {
	dbProvider := func() (*sql.DB, error) {
		return nil, check.ErrSimulated
	}
	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.Nil(t, b)
	assert.Equal(t, check.ErrSimulated, err)
}

func TestErrorInPrepareMarkQuery(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare).WillReturnError(check.ErrSimulated)

	mock.ExpectClose()
	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.Nil(t, b)
	assert.Equal(t, check.ErrSimulated, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestErrorInPreparePurgeQuery(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare).WillReturnError(check.ErrSimulated)

	mark.WillBeClosed()
	mock.ExpectClose()
	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.Nil(t, b)
	assert.Equal(t, check.ErrSimulated, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestErrorInPrepareResetQuery(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	purge := mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare).WillReturnError(check.ErrSimulated)

	mark.WillBeClosed()
	purge.WillBeClosed()
	mock.ExpectClose()
	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.Nil(t, b)
	assert.Equal(t, check.ErrSimulated, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

const testMarkQueryLimit = 100

func TestExecuteMark_queryError(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	purge := mock.ExpectPrepare(purgePrepare)
	reset := mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	leaderID, _ := uuid.NewRandom()
	mark.ExpectQuery().WithArgs(leaderID, testMarkQueryLimit).WillReturnError(check.ErrSimulated)

	records, err := b.Mark(leaderID, testMarkQueryLimit)
	assert.Nil(t, records)
	assert.Equal(t, check.ErrSimulated, err)

	mock.ExpectClose()
	mark.WillBeClosed()
	purge.WillBeClosed()
	reset.WillBeClosed()
	b.Dispose()
	assert.Nil(t, mock.ExpectationsWereMet())
}

// Tests error when one of the columns is of the wrong data type.
func TestExecuteMarkQuery_scanError(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	leaderID, _ := uuid.NewRandom()
	rows := sqlmock.NewRows([]string{
		"id",
		"create_time",
		"kafka_topic",
		"kafka_key",
		"kafka_value",
		"kafka_header_keys",
		"kafka_header_values",
		"leader_id",
	})
	rows.AddRow("non-int", "", "", "", "", pq.Array([]string{"some-key"}), pq.Array([]string{"some-value"}), leaderID)
	mark.ExpectQuery().WithArgs(leaderID, testMarkQueryLimit).WillReturnRows(rows)

	records, err := b.Mark(leaderID, testMarkQueryLimit)
	assert.Nil(t, records)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "Scan error on column")
	}
}

func TestExecuteMark_success(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	leaderID, _ := uuid.NewRandom()
	exp := []OutboxRecord{
		{
			ID:         77,
			CreateTime: time.Now(),
			KafkaTopic: "kafka_topic",
			KafkaKey:   "kafka_key",
			KafkaValue: StringPtr("kafka_value"),
			KafkaHeaders: KafkaHeaders{
				KafkaHeader{Key: "some-key", Value: "some-value"},
			},
			LeaderID: nil,
		},
		{
			ID:           78,
			CreateTime:   time.Now(),
			KafkaTopic:   "kafka_topic",
			KafkaKey:     "kafka_key",
			KafkaValue:   StringPtr("kafka_value"),
			KafkaHeaders: KafkaHeaders{},
			LeaderID:     nil,
		},
	}
	reverse := func(recs []OutboxRecord) []OutboxRecord {
		reversed := make([]OutboxRecord, len(recs))
		for i, j := len(recs)-1, 0; i >= 0; i, j = i-1, j+1 {
			reversed[i] = recs[j]
		}
		return reversed
	}

	rows := sqlmock.NewRows([]string{
		"id",
		"create_time",
		"kafka_topic",
		"kafka_key",
		"kafka_value",
		"kafka_header_keys",
		"kafka_header_values",
		"leader_id",
	})
	// Reverse the order before returning to test the sorter inside the marker implementation.
	for _, expRec := range reverse(exp) {
		headerKeys, headerValues := flattenHeaders(expRec.KafkaHeaders)
		rows.AddRow(
			expRec.ID,
			expRec.CreateTime,
			expRec.KafkaTopic,
			expRec.KafkaKey,
			expRec.KafkaValue,
			pq.Array(headerKeys),
			pq.Array(headerValues),
			expRec.LeaderID,
		)
	}
	mark.ExpectQuery().WithArgs(leaderID, testMarkQueryLimit).WillReturnRows(rows)

	records, err := b.Mark(leaderID, testMarkQueryLimit)
	assert.Nil(t, err)
	assert.ElementsMatch(t, []interface{}{exp[0], exp[1]}, records)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecuteMark_headerLengthMismatch(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mark := mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	leaderID, _ := uuid.NewRandom()

	rows := sqlmock.NewRows([]string{
		"id",
		"create_time",
		"kafka_topic",
		"kafka_key",
		"kafka_value",
		"kafka_header_keys",
		"kafka_header_values",
		"leader_id",
	})
	rows.AddRow(
		1,
		time.Now(),
		"some-topic",
		"some-key",
		"some-value",
		pq.Array([]string{"k0"}),
		pq.Array([]string{"v0", "v1"}),
		leaderID,
	)
	mark.ExpectQuery().WithArgs(leaderID, testMarkQueryLimit).WillReturnRows(rows)

	records, err := b.Mark(leaderID, testMarkQueryLimit)
	assert.Nil(t, records)
	require.NotNil(t, err)
	assert.Equal(t, "unequal number of header keys (1) and values (2)", err.Error())
}

func flattenHeaders(headers KafkaHeaders) (headerKeys, headerValues []string) {
	if numHeaders := len(headers); numHeaders > 0 {
		headerKeys = make([]string, numHeaders)
		headerValues = make([]string, numHeaders)
		for i, header := range headers {
			headerKeys[i], headerValues[i] = header.Key, header.Value
		}
	} else {
		headerKeys, headerValues = []string{}, []string{}
	}
	return
}

func TestExecutePurge_error(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	purge := mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	purge.ExpectExec().WithArgs(id).WillReturnError(check.ErrSimulated)

	done, err := b.Purge(id)
	assert.False(t, done)
	assert.Equal(t, check.ErrSimulated, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecutePurge_success(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	purge := mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	purge.ExpectExec().WithArgs(id).WillReturnResult(sqlmock.NewResult(-1, 1))

	done, err := b.Purge(id)
	assert.True(t, done)
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecutePurge_notDone(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	purge := mock.ExpectPrepare(purgePrepare)
	mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	purge.ExpectExec().WithArgs(id).WillReturnResult(driver.ResultNoRows)

	done, err := b.Purge(id)
	assert.False(t, done)
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecuteReset_error(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	reset := mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	reset.ExpectExec().WithArgs(id).WillReturnError(check.ErrSimulated)

	done, err := b.Reset(id)
	assert.False(t, done)
	assert.Equal(t, check.ErrSimulated, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecuteReset_success(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	reset := mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	reset.ExpectExec().WithArgs(id).WillReturnResult(sqlmock.NewResult(-1, 1))

	done, err := b.Reset(id)
	assert.True(t, done)
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestExecuteReset_notDone(t *testing.T) {
	dbProvider, mock := pgFixtures()
	mock.ExpectPrepare(markPrepare)
	mock.ExpectPrepare(purgePrepare)
	reset := mock.ExpectPrepare(resetPrepare)

	b, err := newPostgresBinding(dbProvider, outboxTable)
	assert.NotNil(t, b)
	assert.Nil(t, err)

	const id = 77
	reset.ExpectExec().WithArgs(id).WillReturnResult(driver.ResultNoRows)

	done, err := b.Reset(id)
	assert.False(t, done)
	assert.Nil(t, err)
	assert.Nil(t, mock.ExpectationsWereMet())
}

func TestRealPostgresBinding(t *testing.T) {
	b, err := NewPostgresBinding("***corrupt connection info string***", outboxTable)
	assert.Nil(t, b)
	assert.NotNil(t, err)
}
