package stasher

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/obsidiandynamics/goharvest"
	"github.com/stretchr/testify/require"
)

const (
	testTopic       = "topic"
	testKey         = "key"
	testValue       = "value"
	testHeaderKey   = "header-key"
	testHeaderValue = "header-value"
	testInsertQuery = "-- insert query"
)

func TestStash_withHeaders(t *testing.T) {
	s := New("outbox")

	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.Nil(t, err)

	mock.ExpectExec(testInsertQuery).
		WithArgs(testTopic, testKey, testValue, pq.Array([]string{testHeaderKey}), pq.Array([]string{testHeaderValue})).
		WillReturnResult(sqlmock.NewResult(-1, 1))
	err = s.Stash(tx, goharvest.OutboxRecord{
		KafkaTopic: testTopic,
		KafkaKey:   testKey,
		KafkaValue: goharvest.String(testValue),
		KafkaHeaders: goharvest.KafkaHeaders{
			{Key: testHeaderKey, Value: testHeaderValue},
		},
	})
	require.Nil(t, err)

	require.Nil(t, mock.ExpectationsWereMet())
}

func TestStash_withoutHeaders(t *testing.T) {
	s := New("outbox")

	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.Nil(t, err)

	mock.ExpectExec(testInsertQuery).
		WithArgs(testTopic, testKey, testValue, pq.Array([]string{}), pq.Array([]string{})).
		WillReturnResult(sqlmock.NewResult(-1, 1))
	err = s.Stash(tx, goharvest.OutboxRecord{
		KafkaTopic: testTopic,
		KafkaKey:   testKey,
		KafkaValue: goharvest.String(testValue),
	})
	require.Nil(t, err)

	require.Nil(t, mock.ExpectationsWereMet())
}

func TestStash_prepare(t *testing.T) {
	s := New("outbox")

	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	mock.ExpectBegin()
	tx, err := db.Begin()
	require.Nil(t, err)

	mock.ExpectPrepare(testInsertQuery)
	prestash, err := s.Prepare(tx)
	require.Nil(t, err)
	require.NotNil(t, prestash)

	mock.ExpectExec(testInsertQuery).
		WithArgs(testTopic, testKey, testValue, pq.Array([]string{}), pq.Array([]string{})).
		WillReturnResult(sqlmock.NewResult(-1, 1))
	err = prestash.Stash(goharvest.OutboxRecord{
		KafkaTopic: testTopic,
		KafkaKey:   testKey,
		KafkaValue: goharvest.String(testValue),
	})
	require.Nil(t, err)

	require.Nil(t, mock.ExpectationsWereMet())
}
