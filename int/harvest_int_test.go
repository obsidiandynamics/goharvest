package int

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/goharvest/stasher"
	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/diags"
	"github.com/obsidiandynamics/libstdgo/fault"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/obsidiandynamics/libstdgo/scribe/overlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type externals struct {
	cons  *kafka.Consumer
	admin *kafka.AdminClient
	db    *sql.DB
}

const (
	kafkaNamespace                = "goharvest_test"
	topic                         = kafkaNamespace + ".topic"
	partitions                    = 10
	dbSchema                      = "goharvest_test"
	outboxTable                   = dbSchema + ".outbox"
	leaderTopic                   = kafkaNamespace + ".neli"
	leaderGroupID                 = kafkaNamespace + ".group"
	receiverGroupID               = kafkaNamespace + ".receiver_group"
	bootstrapServers              = "localhost:9092"
	dataSource                    = "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable"
	generateInterval              = 5 * time.Millisecond
	generateRecordsPerTxn         = 20
	generateMinRecords            = 100
	generateUniqueKeys            = 10
	receiverPollDuration          = 500 * time.Millisecond
	receiverNoMessagesWarningTime = 10 * time.Second
	waitTimeout                   = 90 * time.Second
)

var logger = overlog.New(overlog.StandardFormat())
var scr = scribe.New(overlog.Bind(logger))

func openExternals() externals {
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"group.id":                 receiverGroupID,
		"enable.auto.commit":       true,
		"auto.offset.reset":        "earliest",
		"socket.timeout.ms":        10000,
		"allow.auto.create.topics": true,
		// "debug":              "all",
	})
	if err != nil {
		panic(err)
	}

	admin, err := kafka.NewAdminClientFromConsumer(cons)
	if err != nil {
		panic(err)
	}
	for {
		result, err := admin.CreateTopics(context.Background(), []kafka.TopicSpecification{
			{
				Topic:             topic,
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			},
		})
		if err != nil {
			if isFatalError(err) {
				panic(err)
			} else {
				// Allow for timeouts and other non-fatal errors.
				scr.W()("Non-fatal error creating topic: %v", err)
			}
		} else {
			if result[0].Error.Code() == kafka.ErrTopicAlreadyExists {
				scr.I()("Topic %s already exists", topic)
			} else if result[0].Error.Code() != kafka.ErrNoError {
				panic(result[0].Error)
			}
			break
		}
	}

	db, err := sql.Open("postgres", dataSource)
	if err != nil {
		panic(err)
	}

	const ddlTemplate = `
		CREATE SCHEMA IF NOT EXISTS %s;
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id                  BIGSERIAL PRIMARY KEY,
			create_time         TIMESTAMP WITH TIME ZONE NOT NULL,
			kafka_topic         VARCHAR(249) NOT NULL,
			kafka_key           VARCHAR(5) NOT NULL,
			kafka_value         VARCHAR(50),
			kafka_header_keys   TEXT[] NOT NULL,
			kafka_header_values TEXT[] NOT NULL,
			leader_id           UUID
		)
	`
	_, err = db.Exec(fmt.Sprintf(ddlTemplate, dbSchema, outboxTable, outboxTable))
	if err != nil {
		panic(err)
	}

	return externals{cons, admin, db}
}

func (x *externals) close() {
	x.cons.Close()
	x.db.Close()
	x.admin.Close()
}

func wait(t check.Tester) check.Timesert {
	return check.Wait(t, waitTimeout)
}

func TestOneNode_withFailures(t *testing.T) {
	test(t, 1, 5*time.Second, ProducerFaultSpecs{
		OnProduce:  fault.Spec{Cnt: fault.Random(0.02), Err: check.ErrSimulated},
		OnDelivery: fault.Spec{Cnt: fault.Random(0.02), Err: check.ErrSimulated},
	})
}

func TestFourNodes_withFailures(t *testing.T) {
	test(t, 4, 5*time.Second, ProducerFaultSpecs{
		OnProduce:  fault.Spec{Cnt: fault.Random(0.02), Err: check.ErrSimulated},
		OnDelivery: fault.Spec{Cnt: fault.Random(0.02), Err: check.ErrSimulated},
	})
}

func TestEightNodes_withoutFailures(t *testing.T) {
	test(t, 8, 2*time.Second, ProducerFaultSpecs{})
}

func test(t *testing.T, numHarvests int, spawnInterval time.Duration, producerFaultSpecs ProducerFaultSpecs) {
	check.RequireLabel(t, "int")
	installSigQuitHandler()

	testID, _ := uuid.NewRandom()
	x := openExternals()
	defer x.close()

	scr.I()("Starting generator")
	generator := startGenerator(t, testID, x.db, generateInterval, generateUniqueKeys)
	defer func() { <-generator.stop() }()

	scr.I()("Starting receiver")
	receiver := startReceiver(t, testID, x.cons)
	defer func() { <-receiver.stop() }()

	harvests := make([]Harvest, numHarvests)
	defer func() {
		for _, h := range harvests {
			if h != nil {
				h.Stop()
			}
		}
	}()
	// Start harvests at a set interval.
	for i := 0; i < numHarvests; i++ {
		config := Config{
			KafkaProducerProvider: FaultyKafkaProducerProvider(StandardKafkaProducerProvider(), producerFaultSpecs),
			Name:                  fmt.Sprintf("harvest-#%d", i+1),
			Scribe:                scribe.New(overlog.Bind(logger)),
			BaseKafkaConfig: KafkaConfigMap{
				"bootstrap.servers": bootstrapServers,
				"socket.timeout.ms": 10000,
			},
			ProducerKafkaConfig: KafkaConfigMap{
				"delivery.timeout.ms": 10000,
				// "debug":               "broker,topic,metadata",
			},
			LeaderTopic:   leaderTopic,
			OutboxTable:   outboxTable,
			LeaderGroupID: leaderGroupID,
			DataSource:    dataSource,
			Limits: Limits{
				MinPollInterval: Duration(100 * time.Millisecond),
				MarkBackoff:     Duration(1 * time.Millisecond),
				IOErrorBackoff:  Duration(1 * time.Millisecond),
			},
		}
		config.Scribe.SetEnabled(scribe.Trace)

		scr.I()("Starting harvest %d/%d", i+1, numHarvests)
		h, err := New(config)
		require.Nil(t, err)
		harvests[i] = h
		require.Nil(t, h.Start())

		scr.I()("Sleeping")
		sleepWithDeadline(spawnInterval)
	}

	// Stop harvests in the order they were started, except for the last one. The last harvest will be stopped
	// only after we've asserted the receipt of all messages.
	for i := 0; i < numHarvests-1; i++ {
		scr.I()("Stopping harvest %d/%d", i+1, numHarvests)
		harvests[i].Stop()
		scr.I()("In-flight records: %d", harvests[i].InFlightRecords())
		sleepWithDeadline(spawnInterval)
	}

	// Wait until the generator produces some records. Once we've produced enough records, stop the
	// generator so that we can assert receipt.
	generator.recs.Fill(generateMinRecords, concurrent.Indefinitely)
	scr.I()("Stopping generator")
	<-generator.stop()
	generated := generator.recs.GetInt()
	scr.I()("Generated %d records", generated)

	// Wait until we received all records. Keep sliding in bite-sized chunks through successive assertions so that, as
	// long as we keep on receiving records, the assertion does not fail. This deals with slow harvesters (when we are
	// simulating lots of faults).
	const waitBatchSize = 100
	for r := waitBatchSize; r < generated; r += waitBatchSize {
		advanced := wait(t).UntilAsserted(func(t check.Tester) {
			assert.GreaterOrEqual(t, receiver.recs.GetInt(), r)
		})
		if !advanced {
			scr.E()("Stack traces:\n%s", diags.DumpAllStacks())
		}
		require.True(t, advanced)
		scr.I()("Received %d messages", r)
	}
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.GreaterOrEqual(t, receiver.recs.GetInt(), generated)
	})
	assert.Equal(t, generated, receiver.recs.GetInt())
	scr.I()("Stopping receiver")
	<-receiver.stop()

	// Stop the last harvest as we've already received all messages and there's nothing more to publish.
	scr.I()("Stopping harvest %d/%d", numHarvests, numHarvests)
	harvests[numHarvests-1].Stop()

	// Await harvests.
	for i, h := range harvests {
		scr.I()("Awaiting harvest %d/%d", i+1, numHarvests)
		assert.Nil(t, h.Await())
	}
	scr.I()("Done")
}

func sleepWithDeadline(duration time.Duration) {
	beforeSleep := time.Now()
	time.Sleep(duration)
	if elapsed := time.Now().Sub(beforeSleep); elapsed > 2*duration {
		scr.W()("Sleep deadline exceeded; expected %v but slept for %v", duration, elapsed)
	}
}

type generator struct {
	cancel  context.CancelFunc
	recs    concurrent.AtomicCounter
	stopped chan int
}

func (g generator) stop() chan int {
	g.cancel()
	return g.stopped
}

func startGenerator(t *testing.T, testID uuid.UUID, db *sql.DB, interval time.Duration, keys int) generator {
	st := stasher.New(outboxTable)
	ctx, cancel := concurrent.Forever(context.Background())
	recs := concurrent.NewAtomicCounter()
	stopped := make(chan int, 1)

	go func() {
		defer scr.T()("Generator exiting")
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var tx *sql.Tx
		defer func() {
			err := finaliseTx(t, tx)
			if err != nil {
				scr.E()("Could not finalise transaction: %v", err)
				t.Errorf("Could not finalise transaction: %v", err)
			}
		}()

		var pre stasher.PreStash
		seq := 0
		for {
			if seq%generateRecordsPerTxn == 0 {
				err := finaliseTx(t, tx)
				if err != nil {
					scr.E()("Could not finalise transaction: %v", err)
					t.Errorf("Could not finalise transaction: %v", err)
					return
				}

				newTx, err := db.Begin()
				tx = newTx
				if err != nil {
					scr.E()("Could not begin transaction: %v", err)
					t.Errorf("Could not begin transaction: %v", err)
					return
				}
				pre, err = st.Prepare(tx)
				if err != nil {
					scr.E()("Could not prepare: %v", err)
					t.Errorf("Could not prepare: %v", err)
					return
				}
			}

			testIDStr := testID.String()
			rec := OutboxRecord{
				KafkaTopic: topic,
				KafkaKey:   strconv.Itoa(seq % keys),
				KafkaValue: String(testIDStr + "_" + strconv.Itoa(seq)),
				KafkaHeaders: KafkaHeaders{
					KafkaHeader{Key: "testId", Value: testIDStr},
				},
			}
			err := pre.Stash(rec)
			if err != nil {
				scr.E()("Could not stash: %v", err)
				t.Errorf("Could not stash: %v", err)
				return
			}

			seq = int(recs.Inc())
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return generator{cancel, recs, stopped}
}

func finaliseTx(t *testing.T, tx *sql.Tx) error {
	if tx != nil {
		return tx.Commit()
	}
	return nil
}

type receiver struct {
	cancel   context.CancelFunc
	received map[string]int
	recs     concurrent.AtomicCounter
	stopped  chan int
}

func (r receiver) stop() chan int {
	r.cancel()
	return r.stopped
}

func startReceiver(t *testing.T, testID uuid.UUID, cons *kafka.Consumer) receiver {
	received := make(map[string]int)
	ctx, cancel := concurrent.Forever(context.Background())
	recs := concurrent.NewAtomicCounter()
	stopped := make(chan int, 1)

	go func() {
		defer scr.T()("Receiver exiting")
		defer close(stopped)

		successiveTimeouts := 0
		resetTimeouts := func() {
			if successiveTimeouts > 0 {
				successiveTimeouts = 0
			}
		}

		err := cons.Subscribe(topic, func(_ *kafka.Consumer, event kafka.Event) error {
			switch e := event.(type) {
			case kafka.AssignedPartitions:
				resetTimeouts()
				scr.I()("Receiver: assigned partitions %v", e.Partitions)
			case kafka.RevokedPartitions:
				resetTimeouts()
				scr.I()("Receiver: revoked partitions %v", e.Partitions)
			}
			return nil
		})
		if err != nil {
			scr.E()("Could not subscribe: %v", err)
			t.Errorf("Could not subscribe: %v", err)
			return
		}

		lastMessageReceivedTime := time.Now()
		messageAbsencePrinted := false
		expectedTestID := testID.String()
		const partitions = 64
		lastReceivedOffsets := make([]kafka.Offset, partitions)
		for i := 0; i < partitions; i++ {
			lastReceivedOffsets[i] = kafka.Offset(-1)
		}

		for {
			msg, err := cons.ReadMessage(receiverPollDuration)
			if err != nil {
				if isFatalError(err) {
					scr.E()("Fatal error during poll: %v", err)
					t.Errorf("Fatal error during poll: %v", err)
					return
				} else if !isTimedOutError(err) {
					scr.W()("Error during poll: %v", err)
				} else {
					successiveTimeouts++
					logger.Raw(".")
				}
			}

			if msg != nil {
				if msg.TopicPartition.Offset <= lastReceivedOffsets[msg.TopicPartition.Partition] {
					scr.D()("Skipping duplicate delivery at offset %d", msg.TopicPartition.Offset)
					continue
				}
				lastReceivedOffsets[msg.TopicPartition.Partition] = msg.TopicPartition.Offset
				lastMessageReceivedTime = time.Now()
				messageAbsencePrinted = false

				resetTimeouts()

				valueFrags := strings.Split(string(msg.Value), "_")
				if len(valueFrags) != 2 {
					scr.E()("invalid value '%s'", string(msg.Value))
					t.Errorf("invalid value '%s'", string(msg.Value))
					return
				}
				receivedTestID, value := valueFrags[0], valueFrags[1]
				if receivedTestID != expectedTestID {
					scr.I()("Skipping %s (test ID %s)", string(msg.Value), expectedTestID)
					continue
				}
				key := string(msg.Key)

				receivedSeq, err := strconv.Atoi(value)
				if err != nil {
					scr.E()("Could not convert message value to sequence: '%s'", value)
					t.Errorf("Could not convert message value to sequence: '%s'", value)
					return
				}

				if assert.Equal(t, 1, len(msg.Headers)) {
					assert.Equal(t, expectedTestID, string(msg.Headers[0].Value))
				}

				if existingSeq, ok := received[key]; ok {
					if assert.GreaterOrEqual(t, receivedSeq, existingSeq) {
						if receivedSeq > existingSeq {
							received[key] = receivedSeq
							recs.Inc()
						} else {
							scr.I()("Received duplicate %d for key %s (this is okay)", existingSeq, key)
						}
					} else {
						scr.E()("Received records out of order, %d is behind %d", receivedSeq, existingSeq)
						t.Errorf("Received records out of order, %d is behind %d", receivedSeq, existingSeq)
					}
				} else {
					keyInt, err := strconv.Atoi(key)
					if err != nil {
						scr.E()("Could not convert message key '%s'", key)
						t.Errorf("Could not convert message key '%s'", key)
						return
					}
					if assert.Equal(t, keyInt, receivedSeq) {
						recs.Inc()
						received[key] = receivedSeq
					}
				}
			} else {
				elapsed := time.Now().Sub(lastMessageReceivedTime)
				if elapsed > receiverNoMessagesWarningTime && !messageAbsencePrinted {
					scr.W()("No messages received since %v", lastMessageReceivedTime)
					messageAbsencePrinted = true
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	return receiver{cancel, received, recs, stopped}
}

func isTimedOutError(err error) bool {
	kafkaError, ok := err.(kafka.Error)
	return ok && kafkaError.Code() == kafka.ErrTimedOut
}

func isFatalError(err error) bool {
	kafkaError, ok := err.(kafka.Error)
	return ok && kafkaError.IsFatal()
}

var sigQuitHandlerInstalled = concurrent.NewAtomicCounter()

func installSigQuitHandler() {
	if sigQuitHandlerInstalled.CompareAndSwap(0, 1) {
		sig := make(chan os.Signal, 1)
		go func() {
			signal.Notify(sig, syscall.SIGQUIT)
			select {
			case <-sig:
				scr.I()("Stack\n%s", diags.DumpAllStacks())
			}
		}()
	}
}
