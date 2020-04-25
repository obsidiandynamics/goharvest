package goharvest

import (
	"database/sql"
	"log"
	"testing"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	logrus "github.com/sirupsen/logrus"
)

func Example() {
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
	config := Config{
		BaseKafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		DataSource: dataSource,
	}

	// Create a new harvester.
	harvest, err := New(config)
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

func TestExample(t *testing.T) {
	check.RunTargetted(t, Example)
}

func Example_withCustomLogger() {
	// Example: Configure GoHarvest with a Logrus binding for Scribe.

	log := logrus.StandardLogger()
	log.SetLevel(logrus.DebugLevel)

	// Configure the custom logger using a binding.
	config := Config{
		BaseKafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		Scribe:     scribe.New(scribelogrus.Bind()),
		DataSource: "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable",
	}

	// Create a new harvester.
	harvest, err := New(config)
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

func TestExample_withCustomLogger(t *testing.T) {
	check.RunTargetted(t, Example_withCustomLogger)
}

func Example_withSaslSslAndCustomProducerConfig() {
	// Example: Using Kafka with sasl_ssl for authentication and encryption.

	config := Config{
		BaseKafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9094",
			"security.protocol": "sasl_ssl",
			"ssl.ca.location":   "ca-cert.pem",
			"sasl.mechanism":    "SCRAM-SHA-512",
			"sasl.username":     "alice",
			"sasl.password":     "alice-secret",
		},
		ProducerKafkaConfig: KafkaConfigMap{
			"compression.type": "lz4",
		},
		DataSource: "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable",
	}

	// Create a new harvester.
	harvest, err := New(config)
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

func TestExample_withSaslSslAndCustomProducerConfig(t *testing.T) {
	check.RunTargetted(t, Example_withSaslSslAndCustomProducerConfig)
}

func Example_withEventHandler() {
	// Example: Registering a custom event handler to get notified of leadership changes and metrics.

	log := logrus.StandardLogger()
	log.SetLevel(logrus.TraceLevel)
	config := Config{
		BaseKafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9092",
		},
		DataSource: "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable",
		Scribe:     scribe.New(scribelogrus.Bind()),
	}

	// Create a new harvester and register an event hander.
	harvest, err := New(config)
	if err != nil {
		panic(err)
	}

	harvest.SetEventHandler(func(e Event) {
		switch event := e.(type) {
		case LeaderAcquired:
			log.Infof("Got event: leader acquired: %v", event.LeaderID())
		case LeaderRefreshed:
			log.Infof("Got event: leader refreshed: %v", event.LeaderID())
		case LeaderRevoked:
			log.Infof("Got event: leader revoked")
		case LeaderFenced:
			log.Infof("Got event: leader fenced")
		case *MeterRead:
			log.Infof("Got event: meter read: %v", event.Stats())
		}
	})

	// Start harvesting.
	err = harvest.Start()
	if err != nil {
		panic(err)
	}

	// Wait indefinitely for it to end.
	log.Fatal(harvest.Await())
}

func TestExample_withEventHandler(t *testing.T) {
	check.RunTargetted(t, Example_withEventHandler)
}
