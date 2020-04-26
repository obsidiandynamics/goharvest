package goharvest

import (
	"database/sql"
	"log"
	"testing"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
	"github.com/sirupsen/logrus"
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

	// Configure the harvester. It will use its own database and Kafka connections under the hood.
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

	// Start harvesting in the background.
	err = harvest.Start()
	if err != nil {
		panic(err)
	}

	// Wait indefinitely for the harvester to end.
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

	// Register a handler callback, invoked when an event occurs within goharvest.
	// The callback is completely optional; it lets the application piggy-back on leader
	// status updates, in case it needs to schedule some additional work (other than
	// harvesting outbox records) that should only be run on one process at any given time.
	harvest.SetEventHandler(func(e Event) {
		switch event := e.(type) {
		case LeaderAcquired:
			// The application may initialise any state necessary to perform work as a leader.
			log.Infof("Got event: leader acquired: %v", event.LeaderID())
		case LeaderRefreshed:
			// Indicates that a new leader ID was generated, as a result of having to remark
			// a record (typically as due to an earlier delivery error). This is purely
			// informational; there is nothing an application should do about this, other
			// than taking note of the new leader ID if it has come to rely on it.
			log.Infof("Got event: leader refreshed: %v", event.LeaderID())
		case LeaderRevoked:
			// The application may block the callback until it wraps up any in-flight
			// activity. Only upon returning from the callback, will a new leader be elected.
			log.Infof("Got event: leader revoked")
		case LeaderFenced:
			// The application must immediately terminate any ongoing activity, on the assumption
			// that another leader may be imminently elected. Unlike the handling of LeaderRevoked,
			// blocking in the callback will not prevent a new leader from being elected.
			log.Infof("Got event: leader fenced")
		case MeterRead:
			// Periodic statistics regarding the harvester's throughput.
			log.Infof("Got event: meter read: %v", event.Stats())
		}
	})

	// Start harvesting in the background.
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
