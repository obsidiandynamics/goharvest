package stasher

import (
	"database/sql"
	"testing"

	"github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/libstdgo/check"
)

func Example() {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	st := New("outbox")

	// Begin a transaction.
	tx, _ := db.Begin()
	defer tx.Rollback()

	// Update other database entities in transaction scope.
	// ...

	// Stash an outbox record for subsequent harvesting.
	err = st.Stash(tx, goharvest.OutboxRecord{
		KafkaTopic: "my-app.topic",
		KafkaKey:   "hello",
		KafkaValue: goharvest.String("world"),
		KafkaHeaders: goharvest.KafkaHeaders{
			{Key: "applicationId", Value: "my-app"},
		},
	})
	if err != nil {
		panic(err)
	}

	// Commit the transaction.
	tx.Commit()
}

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}

func Example_prepare() {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	st := New("outbox")

	// Begin a transaction.
	tx, _ := db.Begin()
	defer tx.Rollback()

	// Update other database entities in transaction scope.
	// ...

	// Formulates a prepared statement that may be reused within the scope of the transaction.
	prestash, _ := st.Prepare(tx)

	// Publish a bunch of messages using the same prepared statement.
	for i := 0; i < 10; i++ {
		// Stash an outbox record for subsequent harvesting.
		err = prestash.Stash(goharvest.OutboxRecord{
			KafkaTopic: "my-app.topic",
			KafkaKey:   "hello",
			KafkaValue: goharvest.String("world"),
			KafkaHeaders: goharvest.KafkaHeaders{
				{Key: "applicationId", Value: "my-app"},
			},
		})
		if err != nil {
			panic(err)
		}
	}

	// Commit the transaction.
	tx.Commit()
}

func TestExample_prepare(t *testing.T) {
	check.RunTargetted(t, Example_prepare)
}
