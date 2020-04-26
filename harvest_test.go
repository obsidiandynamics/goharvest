package goharvest

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/obsidiandynamics/goneli"
	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func wait(t check.Tester) check.Timesert {
	return check.Wait(t, 10*time.Second)
}

// Aggressive limits used for (fast) testing and without send concurrency to simplify assertions.
func testLimits() Limits {
	return Limits{
		IOErrorBackoff:     Duration(1 * time.Millisecond),
		PollDuration:       Duration(1 * time.Millisecond),
		MinPollInterval:    Duration(1 * time.Millisecond),
		MaxPollInterval:    Duration(60 * time.Second),
		HeartbeatTimeout:   Duration(60 * time.Second),
		DrainInterval:      Duration(60 * time.Second),
		QueueTimeout:       Duration(60 * time.Second),
		MarkBackoff:        Duration(1 * time.Millisecond),
		MaxInFlightRecords: Int(math.MaxInt64),
		SendConcurrency:    Int(1),
		SendBuffer:         Int(0),
	}
}

type fixtures struct {
	producerMockSetup producerMockSetup
}

func (f *fixtures) setDefaults() {
	if f.producerMockSetup == nil {
		f.producerMockSetup = func(prodMock *prodMock) {}
	}
}

type producerMockSetup func(prodMock *prodMock)

func (f fixtures) create() (scribe.MockScribe, *dbMock, *goneli.MockNeli, Config) {
	f.setDefaults()
	m := scribe.NewMock()

	db := &dbMock{}
	db.fillDefaults()

	var neli goneli.MockNeli

	config := Config{
		Limits:                  testLimits(),
		Scribe:                  scribe.New(m.Factories()),
		DatabaseBindingProvider: mockDatabaseBindingProvider(db),
		NeliProvider: func(config goneli.Config, barrier goneli.Barrier) (goneli.Neli, error) {
			n, err := goneli.NewMock(goneli.MockConfig{
				MinPollInterval: config.MinPollInterval,
			}, barrier)
			if err != nil {
				panic(err)
			}
			neli = n
			return n, nil
		},
		KafkaProducerProvider: func(conf *KafkaConfigMap) (KafkaProducer, error) {
			prod := &prodMock{}
			prod.fillDefaults()
			f.producerMockSetup(prod)
			return prod, nil
		},
	}
	config.Scribe.SetEnabled(scribe.All)

	return m, db, &neli, config
}

type testEventHandler struct {
	mutex  sync.Mutex
	events []Event
}

func (c *testEventHandler) handler() EventHandler {
	return func(e Event) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.events = append(c.events, e)
	}
}

func (c *testEventHandler) list() []Event {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	eventsCopy := make([]Event, len(c.events))
	copy(eventsCopy, c.events)
	return eventsCopy
}

func (c *testEventHandler) length() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.events)
}

func TestCorrectInitialisation(t *testing.T) {
	_, db, neli, config := fixtures{}.create()

	var givenDataSource string
	var givenOutboxTable string

	config.DatabaseBindingProvider = func(dataSource string, outboxTable string) (DatabaseBinding, error) {
		givenDataSource = dataSource
		givenOutboxTable = outboxTable
		return db, nil
	}
	config.DataSource = "test data source"
	config.OutboxTable = "test table name"
	config.LeaderGroupID = "test leader group ID"
	config.BaseKafkaConfig = KafkaConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	h, err := New(config)
	require.Nil(t, err)
	assert.Equal(t, Created, h.State())
	assertNoError(t, h.Start)
	assert.Equal(t, Running, h.State())

	assert.Equal(t, config.DataSource, givenDataSource)
	assert.Equal(t, config.OutboxTable, givenOutboxTable)

	h.Stop()
	assert.Nil(t, h.Await())
	assert.Equal(t, Stopped, h.State())

	assert.Equal(t, 1, db.c.Dispose.GetInt())
	assert.Equal(t, goneli.Closed, (*neli).State())
}

func TestConfigError(t *testing.T) {
	h, err := New(Config{
		Limits: Limits{
			IOErrorBackoff: Duration(-1),
		},
	})
	assert.Nil(t, h)
	assert.NotNil(t, err)
}

func TestErrorDuringDBInitialisation(t *testing.T) {
	_, _, _, config := fixtures{}.create()

	config.DatabaseBindingProvider = func(dataSource string, outboxTable string) (DatabaseBinding, error) {
		return nil, check.ErrSimulated
	}
	h, err := New(config)
	require.Nil(t, err)

	assertErrorContaining(t, h.Start, "simulated")
	assert.Equal(t, Created, h.State())
}

func TestErrorDuringNeliInitialisation(t *testing.T) {
	_, db, _, config := fixtures{}.create()

	config.NeliProvider = func(config goneli.Config, barrier goneli.Barrier) (goneli.Neli, error) {
		return nil, check.ErrSimulated
	}
	h, err := New(config)
	require.Nil(t, err)

	assertErrorContaining(t, h.Start, "simulated")
	assert.Equal(t, Created, h.State())
	assert.Equal(t, 1, db.c.Dispose.GetInt())
}

func TestErrorDuringProducerConfiguration(t *testing.T) {
	_, _, _, config := fixtures{}.create()

	config.ProducerKafkaConfig = KafkaConfigMap{
		"enable.idempotence": false,
	}
	h, err := New(config)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "cannot override configuration 'enable.idempotence'")
	assert.Nil(t, h)
}

func TestErrorDuringProducerInitialisation(t *testing.T) {
	m, db, neli, config := fixtures{}.create()

	config.KafkaProducerProvider = func(conf *KafkaConfigMap) (KafkaProducer, error) {
		return nil, check.ErrSimulated
	}
	h, err := New(config)
	require.Nil(t, err)

	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Induce leadership and wait until leader.
	(*neli).AcquireLeader()
	wait(t).Until(h.IsLeader)
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.Equal(t, 1, eh.length())
	})

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Error)).
		Having(scribe.MessageEqual("Caught panic in send cell: simulated")).
		Passes(scribe.Count(1)))

	// Having detected a panic, it should self-destruct
	assertErrorContaining(t, h.Await, "simulated")

	assert.Equal(t, 1, db.c.Dispose.GetInt())
	assert.Equal(t, (*neli).State(), goneli.Closed)
}

func TestUncaughtPanic_backgroundPoller(t *testing.T) {
	m, _, neli, config := fixtures{}.create()

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	(*neli).PulseError(check.ErrSimulated)

	// Having detected a panic, it should self-destruct
	assertErrorContaining(t, h.Await, "simulated")
	assert.Equal(t, 0, eh.length())

	t.Log(m.Entries().List())
	m.Entries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Starting background poller")).
		Assert(t, scribe.Count(1))

	m.Entries().
		Having(scribe.LogLevel(scribe.Error)).
		Having(scribe.MessageEqual("Caught panic in background poller: simulated")).
		Assert(t, scribe.Count(1))
}

func TestUncaughtPanic_backgroundDeliveryHandler(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(prodMock *prodMock) {
		prodRef.Set(prodMock)
	}}.create()

	db.f.Reset = func(m *dbMock, id int64) (bool, error) {
		panic(check.ErrSimulated)
	}

	h, err := New(config)
	require.Nil(t, err)
	assertNoError(t, h.Start)

	// Induce leadership and await
	(*neli).AcquireLeader()
	wait(t).Until(h.IsLeader)

	// Feed a delivery event to cause a DB reset query
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prodRef.Get().(*prodMock).events <- message(OutboxRecord{ID: 777}, check.ErrSimulated)

	// Having detected a panic, it should self-destruct
	assertErrorContaining(t, h.Await, "simulated")

	t.Log(m.Entries().List())
	m.Entries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Starting background delivery handler")).
		Assert(t, scribe.Count(1))

	m.Entries().
		Having(scribe.LogLevel(scribe.Error)).
		Having(scribe.MessageEqual("Caught panic in background delivery handler: simulated")).
		Assert(t, scribe.Count(1))
}

func TestBasicLeaderElectionAndRevocation(t *testing.T) {
	m, _, neli, config := fixtures{}.create()

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Starts off in a non-leader state
	assert.Equal(t, false, h.IsLeader())
	assert.Nil(t, h.LeaderID())

	// Assign leadership via the rebalance listener and wait for the assignment to take effect
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isTrue(h.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual(fmt.Sprintf("Elected as leader, ID: %s", h.LeaderID()))).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 1, eh.length()) {
			e := eh.list()[0].(LeaderAcquired)
			assert.Equal(t, e.LeaderID(), *(h.LeaderID()))
		}
	})

	// Revoke leadership via the rebalance listener and await its effect
	(*neli).RevokeLeader()
	wait(t).UntilAsserted(isFalse(h.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual("Lost leader status")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 2, eh.length()) {
			_ = eh.list()[1].(LeaderRevoked)
		}
	})

	// Reassign leadership via the rebalance listener and wait for the assignment to take effect
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isTrue(h.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageEqual(fmt.Sprintf("Elected as leader, ID: %s", h.LeaderID()))).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 3, eh.length()) {
			e := eh.list()[2].(LeaderAcquired)
			assert.Equal(t, e.LeaderID(), *(h.LeaderID()))
		}
	})

	// Fence the leader
	(*neli).FenceLeader()
	wait(t).UntilAsserted(isFalse(h.IsLeader))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageEqual("Leader fenced")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))
	m.Reset()
	wait(t).UntilAsserted(func(t check.Tester) {
		if assert.Equal(t, 4, eh.length()) {
			_ = eh.list()[3].(LeaderFenced)
		}
	})

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestMetrics(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	m, _, neli, config := fixtures{producerMockSetup: func(prodMock *prodMock) {
		prodRef.Set(prodMock)
	}}.create()
	config.Limits.MinMetricsInterval = Duration(1 * time.Millisecond)

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Induce leadership and wait for the leadership event
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.Equal(t, 1, eh.length())
	})

	wait(t).UntilAsserted(func(t check.Tester) {
		backlogRecords := generateRecords(1, 0)
		deliverAll(backlogRecords, nil, prodRef.Get().(*prodMock).events)
		if assert.GreaterOrEqual(t, eh.length(), 2) {
			e := eh.list()[1].(MeterRead)
			if stats := e.Stats(); assert.NotNil(t, stats) {
				assert.Equal(t, stats.Name, "throughput")
			}
		}
	})
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageContaining("throughput")).
		Passes(scribe.CountAtLeast(1)))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestHandleNonMessageEvent(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	m, _, neli, config := fixtures{producerMockSetup: func(prodMock *prodMock) {
		prodRef.Set(prodMock)
	}}.create()
	config.Limits.MinMetricsInterval = Duration(1 * time.Millisecond)

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Induce leadership and wait for the leadership event
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod := prodRef.Get().(*prodMock)
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.Equal(t, 1, eh.length())
	})

	prod.events <- kafka.NewError(kafka.ErrAllBrokersDown, "brokers down", false)

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageContaining("Observed event: brokers down")).
		Passes(scribe.CountAtLeast(1)))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestThrottleKeys(t *testing.T) {
	prod := concurrent.NewAtomicReference()
	lastPublished := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		pm.f.Produce = func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error {
			lastPublished.Set(msg)
			return nil
		}
		prod.Set(pm)
	}}.create()

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Starts off with no backlog.
	assert.Equal(t, 0, h.InFlightRecords())

	// Induce leadership and wait until a producer has been spawned.
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prod.Get))

	const backlog = 10
	backlogRecords := generateCyclicKeyedRecords(1, backlog, 0)
	db.markedRecords <- backlogRecords

	// Even though we pushed several records through, they all had a common key, so only one should
	// should be published.
	wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))
	assert.True(t, h.IsLeader()) // should definitely be leader by now
	wait(t).UntilAsserted(intEqual(1, prod.Get().(*prodMock).c.Produce.GetInt))
	msg := lastPublished.Get().(*kafka.Message)
	assert.Equal(t, msg.Value, []byte(*backlogRecords[0].KafkaValue))
	assert.ElementsMatch(t, h.InFlightRecordKeys(), []string{backlogRecords[0].KafkaKey})

	// Drain the in-flight record... another one should then be published.
	deliverAll(backlogRecords[0:1], nil, prod.Get().(*prodMock).events)
	wait(t).UntilAsserted(func(t check.Tester) {
		msg := lastPublished.Get()
		if assert.NotNil(t, msg) {
			assert.Equal(t, msg.(*kafka.Message).Value, []byte(*backlogRecords[1].KafkaValue))
		}
	})

	// Drain the backlog by feeding in delivery confirmations one at a time.
	for i := 1; i < backlog; i++ {
		wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))
		wait(t).UntilAsserted(func(t check.Tester) {
			msg := lastPublished.Get()
			if assert.NotNil(t, msg) {
				assert.Equal(t, []byte(*backlogRecords[i].KafkaValue), msg.(*kafka.Message).Value)
			}
		})
		deliverAll(backlogRecords[i:i+1], nil, prod.Get().(*prodMock).events)
	}

	// Revoke leadership...
	(*neli).RevokeLeader()

	// Wait for the backlog to drain... leadership status will be cleared when done.
	wait(t).Until(check.Not(h.IsLeader))

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageContaining("Lost leader status")).
		Passes(scribe.Count(1)))

	assert.Equal(t, backlog, db.c.Purge.GetInt())
	assert.Equal(t, backlog, prod.Get().(*prodMock).c.Produce.GetInt())
	assert.Equal(t, 0, h.InFlightRecords())

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestPollDeadlineExceeded(t *testing.T) {
	m, db, neli, config := fixtures{}.create()

	config.Limits.DrainInterval = Duration(time.Millisecond)
	config.Limits.MaxPollInterval = Duration(time.Millisecond)
	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Starts off with no backlog.
	assert.Equal(t, 0, h.InFlightRecords())

	// Induce leadership and wait until a producer has been spawned.
	(*neli).AcquireLeader()

	db.markedRecords <- generateCyclicKeyedRecords(1, 2, 0)

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Exceeded poll deadline")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestQueueLimitExceeded(t *testing.T) {
	m, db, neli, config := fixtures{}.create()

	config.Limits.DrainInterval = Duration(time.Millisecond)
	config.Limits.QueueTimeout = Duration(time.Millisecond)
	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Starts off with no backlog.
	assert.Equal(t, 0, h.InFlightRecords())

	// Induce leadership and wait until a producer has been spawned.
	(*neli).AcquireLeader()

	db.markedRecords <- generateCyclicKeyedRecords(1, 2, 0)

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Exceeded message queueing deadline")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestDrainInFlightRecords_failedDelivery(t *testing.T) {
	prod := concurrent.NewAtomicReference()
	lastPublished := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		pm.f.Produce = func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error {
			lastPublished.Set(msg)
			return nil
		}
		prod.Set(pm)
	}}.create()

	h, err := New(config)
	require.Nil(t, err)
	assertNoError(t, h.Start)

	// Starts off with no backlog
	assert.Equal(t, 0, h.InFlightRecords())

	// Induce leadership
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prod.Get))

	// Generate a backlog
	const backlog = 10
	backlogRecords := generateRecords(backlog, 0)
	db.markedRecords <- backlogRecords

	// Wait for the backlog to register.
	wait(t).UntilAsserted(intEqual(backlog, h.InFlightRecords))
	wait(t).UntilAsserted(intEqual(backlog, prod.Get().(*prodMock).c.Produce.GetInt))
	assert.True(t, h.IsLeader()) // should be leader by now

	// Revoke leadership... this will start the backlog drain.
	(*neli).RevokeLeader()

	wait(t).Until(check.Not(h.IsLeader))

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Shutting down send battery")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Debug)).
		Having(scribe.MessageEqual("Send battery terminated")).
		Passes(scribe.Count(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Info)).
		Having(scribe.MessageContaining("Lost leader status")).
		Passes(scribe.Count(1)))
	assert.Equal(t, h.InFlightRecords(), 0)

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestNonFatalErrorInMarkQuery(t *testing.T) {
	m, db, neli, config := fixtures{}.create()

	db.f.Mark = func(m *dbMock, leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
		return nil, check.ErrSimulated
	}

	h, err := New(config)
	require.Nil(t, err)
	assertNoError(t, h.Start)

	// Induce leadership
	(*neli).AcquireLeader()

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Error executing mark query")).
		Passes(scribe.CountAtLeast(1)))
	assert.Equal(t, Running, h.State())

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestErrorInProduce(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	produceError := concurrent.NewAtomicCounter(1) // 1=true, 0=false
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		pm.f.Produce = func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error {
			if produceError.Get() == 1 {
				return kafka.NewError(kafka.ErrFail, "simulated", false)
			}
			return nil
		}
		prodRef.Set(pm)
	}}.create()

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Induce leadership
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod := prodRef.Get().(*prodMock)
	prodRef.Set(nil)

	// Mark one record
	records := generateRecords(1, 0)
	db.markedRecords <- records

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Error publishing record")).
		Passes(scribe.CountAtLeast(1)))
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Refreshed leader ID")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod = prodRef.Get().(*prodMock)

	// Resume normal production... error should clear but the record count should not go up, as
	// there can only be one in-flight record for a given key
	produceError.Set(0)
	db.markedRecords <- records
	wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))
	wait(t).UntilAsserted(func(t check.Tester) {
		assert.ElementsMatch(t, h.InFlightRecordKeys(), []string{records[0].KafkaKey})
	})

	if assert.GreaterOrEqual(t, eh.length(), 2) {
		_ = eh.list()[0].(LeaderAcquired)
		_ = eh.list()[1].(LeaderRefreshed)
	}

	// Feed successful delivery report for the first record
	prod.events <- message(records[0], nil)

	h.Stop()
	assert.Nil(t, h.Await())
}

// Tests remarking by feeding through two records for the same key, forcing them to come through in sequence.
// The first is published, but fails upon delivery, which raises the forceRemark flag.
// As the second on is processed, the forceRemark flag raised by the first should be spotted, and a leader
// refresh should occur.
func TestReset(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	lastPublished := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		pm.f.Produce = func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error {
			lastPublished.Set(msg)
			return nil
		}
		prodRef.Set(pm)
	}}.create()

	h, err := New(config)
	require.Nil(t, err)
	eh := &testEventHandler{}
	h.SetEventHandler(eh.handler())
	assertNoError(t, h.Start)

	// Induce leadership
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod := prodRef.Get().(*prodMock)

	// Mark two records for the same key
	records := generateCyclicKeyedRecords(1, 2, 0)
	db.markedRecords <- records

	// Wait for the backlog to register
	wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))
	wait(t).UntilAsserted(func(t check.Tester) {
		if msg := lastPublished.Get(); assert.NotNil(t, msg) {
			assert.Equal(t, *records[0].KafkaValue, string(msg.(*kafka.Message).Value))
		}
	})

	// Feed an error
	prod.events <- message(records[0], check.ErrSimulated)

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Delivery failed")).
		Passes(scribe.CountAtLeast(1)))

	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Refreshed leader ID")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	wait(t).UntilAsserted(isNotNil(prodRef.Get))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestErrorInPurgeAndResetQueries(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		prodRef.Set(pm)
	}}.create()

	records := generateRecords(2, 0)
	purgeError := concurrent.NewAtomicCounter(1) // 1=true, 0=false
	resetError := concurrent.NewAtomicCounter(1) // 1=true, 0=false
	db.f.Mark = func(m *dbMock, leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
		if db.c.Mark.Get() == 0 {
			return records, nil
		}
		return []OutboxRecord{}, nil
	}
	db.f.Purge = func(m *dbMock, id int64) (bool, error) {
		if purgeError.Get() == 1 {
			return false, check.ErrSimulated
		}
		return true, nil
	}
	db.f.Reset = func(m *dbMock, id int64) (bool, error) {
		if resetError.Get() == 1 {
			return false, check.ErrSimulated
		}
		return true, nil
	}

	h, err := New(config)
	require.Nil(t, err)
	assertNoError(t, h.Start)

	// Induce leadership and await its registration
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod := prodRef.Get().(*prodMock)

	wait(t).UntilAsserted(isTrue(h.IsLeader))
	wait(t).UntilAsserted(intEqual(2, h.InFlightRecords))

	// Feed successful delivery report for the first record
	prod.events <- message(records[0], nil)

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Error executing purge query for record")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	assert.Equal(t, 2, h.InFlightRecords())

	// Resume normal production... error should clear
	purgeError.Set(0)
	wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))

	// Feed failed delivery report for the first record
	prodRef.Get().(*prodMock).events <- message(records[1], kafka.NewError(kafka.ErrFail, "simulated", false))

	// Wait for the error to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Error executing reset query for record")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	assert.Equal(t, 1, h.InFlightRecords())

	// Resume normal production... error should clear
	resetError.Set(0)
	wait(t).UntilAsserted(intEqual(0, h.InFlightRecords))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestIncompletePurgeAndResetQueries(t *testing.T) {
	prodRef := concurrent.NewAtomicReference()
	m, db, neli, config := fixtures{producerMockSetup: func(pm *prodMock) {
		prodRef.Set(pm)
	}}.create()

	records := generateRecords(2, 0)
	db.f.Mark = func(m *dbMock, leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
		if db.c.Mark.Get() == 0 {
			return records, nil
		}
		return []OutboxRecord{}, nil
	}
	db.f.Purge = func(m *dbMock, id int64) (bool, error) {
		return false, nil
	}
	db.f.Reset = func(m *dbMock, id int64) (bool, error) {
		return false, nil
	}

	h, err := New(config)
	require.Nil(t, err)
	assertNoError(t, h.Start)

	// Induce leadership and await its registration
	(*neli).AcquireLeader()
	wait(t).UntilAsserted(isTrue(h.IsLeader))
	wait(t).UntilAsserted(intEqual(2, h.InFlightRecords))
	wait(t).UntilAsserted(isNotNil(prodRef.Get))
	prod := prodRef.Get().(*prodMock)

	// Feed successful delivery report for the first record
	prod.events <- message(records[0], nil)

	// Wait for the warning to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Did not purge record")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	wait(t).UntilAsserted(intEqual(1, h.InFlightRecords))

	// Feed failed delivery report for the first record
	prod.events <- message(records[1], kafka.NewError(kafka.ErrFail, "simulated", false))

	// Wait for the warning to be logged
	wait(t).UntilAsserted(m.ContainsEntries().
		Having(scribe.LogLevel(scribe.Warn)).
		Having(scribe.MessageContaining("Did not reset record")).
		Passes(scribe.CountAtLeast(1)))
	m.Reset()
	assert.Equal(t, Running, h.State())
	wait(t).UntilAsserted(intEqual(0, h.InFlightRecords))

	h.Stop()
	assert.Nil(t, h.Await())
}

func TestEnsureState(t *testing.T) {
	check.ThatPanicsAsExpected(t, check.ErrorContaining("must not be false"), func() {
		ensureState(false, "must not be false")
	})

	ensureState(true, "must not be false")
}

func intEqual(expected int, intSupplier func() int) func(t check.Tester) {
	return func(t check.Tester) {
		assert.Equal(t, expected, intSupplier())
	}
}

func lengthEqual(expected int, sliceSupplier func() []string) func(t check.Tester) {
	return func(t check.Tester) {
		assert.Len(t, sliceSupplier(), expected)
	}
}

func atLeast(min int, f func() int) check.Assertion {
	return func(t check.Tester) {
		assert.GreaterOrEqual(t, f(), min)
	}
}

func isTrue(f func() bool) check.Assertion {
	return func(t check.Tester) {
		assert.True(t, f())
	}
}

func isFalse(f func() bool) check.Assertion {
	return func(t check.Tester) {
		assert.False(t, f())
	}
}

func isNotNil(f func() interface{}) check.Assertion {
	return func(t check.Tester) {
		assert.NotNil(t, f())
	}
}

func assertErrorContaining(t *testing.T, f func() error, substr string) {
	err := f()
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), substr)
	}
}

func assertNoError(t *testing.T, f func() error) {
	err := f()
	require.Nil(t, err)
}

func newTimedOutError() kafka.Error {
	return kafka.NewError(kafka.ErrTimedOut, "Timed out", false)
}

func generatePartitions(indexes ...int32) []kafka.TopicPartition {
	parts := make([]kafka.TopicPartition, len(indexes))
	for i, index := range indexes {
		parts[i] = kafka.TopicPartition{Partition: index}
	}
	return parts
}

func generateRecords(numRecords int, startID int) []OutboxRecord {
	records := make([]OutboxRecord, numRecords)
	now := time.Now()
	for i := 0; i < numRecords; i++ {
		records[i] = OutboxRecord{
			ID:         int64(startID + i),
			CreateTime: now,
			KafkaTopic: "test_topic",
			KafkaKey:   fmt.Sprintf("key-%x", i),
			KafkaValue: String(fmt.Sprintf("value-%x", i)),
			KafkaHeaders: KafkaHeaders{
				KafkaHeader{Key: "ID", Value: strconv.FormatInt(int64(startID+i), 10)},
			},
		}
	}
	return records
}

func generateCyclicKeyedRecords(numKeys int, numRecords int, startID int) []OutboxRecord {
	records := make([]OutboxRecord, numRecords)
	now := time.Now()
	for i := 0; i < numRecords; i++ {
		records[i] = OutboxRecord{
			ID:         int64(startID + i),
			CreateTime: now,
			KafkaTopic: "test_topic",
			KafkaKey:   fmt.Sprintf("key-%x", i%numKeys),
			KafkaValue: String(fmt.Sprintf("value-%x", i)),
			KafkaHeaders: KafkaHeaders{
				KafkaHeader{Key: "ID", Value: strconv.FormatInt(int64(startID+i), 10)},
			},
		}
	}
	return records
}

func message(record OutboxRecord, err error) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &record.KafkaTopic, Error: err},
		Key:            []byte(record.KafkaKey),
		Value:          stringPointerToByteArray(record.KafkaValue),
		Timestamp:      record.CreateTime,
		TimestampType:  kafka.TimestampCreateTime,
		Opaque:         record,
	}
}

func deliverAll(records []OutboxRecord, err error, events chan kafka.Event) {
	for _, record := range records {
		events <- message(record, err)
	}
}
