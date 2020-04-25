package main

import (
	"database/sql"

	"github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/libstdgo/scribe"
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	"github.com/sirupsen/logrus"
)

func main() {
	const dataSource = "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable"

	// Optional: Ensure the database table exists before we start harvesting.
	func() {
		db, err := sql.Open("postgres", dataSource)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS outbox (
				id                  BIGSERIAL PRIMARY KEY,
				create_time         TIMESTAMP WITH TIME ZONE NOT NULL,
				kafka_topic         VARCHAR(249) NOT NULL,
				kafka_key           VARCHAR(100) NOT NULL,  -- pick your own key size
				kafka_value         VARCHAR(10000),         -- pick your own value size
				kafka_header_keys   TEXT[] NOT NULL,
				kafka_header_values TEXT[] NOT NULL,
				leader_id           UUID
			)
		`)
		if err != nil {
			panic(err)
		}
	}()

	// Configure the harvester. It will use its own database connections under the hood.
	log := logrus.StandardLogger()
	log.SetLevel(logrus.DebugLevel)
	config := goharvest.Config{
		BaseKafkaConfig: goharvest.KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		DataSource: dataSource,
		Scribe:     scribe.New(scribelogrus.Bind()),
	}

	// Create a new harvester.
	harvest, err := goharvest.New(config)
	if err != nil {
		panic(err)
	}

	// Start it.
	err = harvest.Start()
	if err != nil {
		panic(err)
	}

	// Wait indefinitely for it to end.
	log.Fatal(harvest.Await())
}
