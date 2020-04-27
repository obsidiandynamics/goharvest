package goharvest

import (
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/obsidiandynamics/goharvest/metric"
	"github.com/obsidiandynamics/goneli"
	"github.com/obsidiandynamics/libstdgo/concurrent"
	"github.com/obsidiandynamics/libstdgo/diags"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var noLeader uuid.UUID

// State of the Harvest instance.
type State int

const (
	// Created — initialised (configured) but not started.
	Created State = iota

	// Running — currently running.
	Running

	// Stopping — in the process of being stopped. I.e. Stop() has been invoked, but workers are still running.
	Stopping

	// Stopped — has been completely disposed of.
	Stopped
)

type tracedPanic struct {
	cause interface{}
	stack string
}

func (e tracedPanic) Error() string {
	return fmt.Sprintf("%v\n%s", e.cause, e.stack)
}

// Harvest performs background harvesting of a transactional outbox table.
type Harvest interface {
	Start() error
	Stop()
	Await() error
	State() State
	IsLeader() bool
	LeaderID() *uuid.UUID
	InFlightRecords() int
	InFlightRecordKeys() []string
	SetEventHandler(eventHandler EventHandler)
}

const watcherTimeout = 60 * time.Second

type harvest struct {
	config              Config
	producerConfigs     KafkaConfigMap
	scribe              scribe.Scribe
	state               concurrent.AtomicReference
	shouldBeRunningFlag concurrent.AtomicCounter
	neli                goneli.Neli
	leaderID            atomic.Value
	db                  DatabaseBinding
	queuedRecords       concurrent.AtomicCounter
	inFlightRecords     concurrent.AtomicCounter
	inFlightKeys        concurrent.Scoreboard
	throughput          *metric.Meter
	throughputLock      sync.Mutex
	panicCause          atomic.Value
	eventHandler        EventHandler
	forceRemarkFlag     concurrent.AtomicCounter
	sendBattery         battery
}

// New creates a new Harvest instance from the supplied config.
func New(config Config) (Harvest, error) {
	config.SetDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}
	h := &harvest{
		config:              config,
		scribe:              config.Scribe,
		state:               concurrent.NewAtomicReference(Created),
		shouldBeRunningFlag: concurrent.NewAtomicCounter(1),
		queuedRecords:       concurrent.NewAtomicCounter(),
		inFlightRecords:     concurrent.NewAtomicCounter(),
		inFlightKeys:        concurrent.NewScoreboard(*config.Limits.SendConcurrency),
		forceRemarkFlag:     concurrent.NewAtomicCounter(),
		eventHandler:        func(e Event) {},
	}
	h.leaderID.Store(noLeader)

	h.producerConfigs = copyKafkaConfig(h.config.BaseKafkaConfig)
	putAllKafkaConfig(h.config.ProducerKafkaConfig, h.producerConfigs)
	err := setKafkaConfigs(h.producerConfigs, KafkaConfigMap{
		"enable.idempotence": true,
	})
	if err != nil {
		return nil, err
	}

	return h, nil
}

// State obtains the present state of this Harvest instance.
func (h *harvest) State() State {
	return h.state.Get().(State)
}

func (h *harvest) logger() scribe.StdLogAPI {
	return h.scribe.Capture(h.scene())
}

func (h *harvest) scene() scribe.Scene {
	return scribe.Scene{Fields: scribe.Fields{
		"name": h.config.Name,
		"lib":  "goharvest",
	}}
}
func (h *harvest) cleanupFailedStart() {
	if h.State() != Created {
		return
	}

	if h.db != nil {
		h.db.Dispose()
	}
}

// Start the harvester.
func (h *harvest) Start() error {
	ensureState(h.State() == Created, "Cannot start at this time")
	defer h.cleanupFailedStart()

	db, err := h.config.DatabaseBindingProvider(h.config.DataSource, h.config.OutboxTable)
	if err != nil {
		return err
	}
	h.db = db

	neliConfig := goneli.Config{
		KafkaConfig:           configToNeli(h.config.BaseKafkaConfig),
		LeaderTopic:           h.config.LeaderTopic,
		LeaderGroupID:         h.config.LeaderGroupID,
		KafkaConsumerProvider: convertKafkaConsumerProvider(h.config.KafkaConsumerProvider),
		KafkaProducerProvider: convertKafkaProducerProvider(h.config.KafkaProducerProvider),
		Scribe:                h.config.Scribe,
		Name:                  h.config.Name,
		PollDuration:          h.config.Limits.PollDuration,
		MinPollInterval:       h.config.Limits.MinPollInterval,
		HeartbeatTimeout:      h.config.Limits.HeartbeatTimeout,
	}
	h.logger().T()("Creating NELI with config %v", neliConfig)
	n, err := h.config.NeliProvider(neliConfig, func(e goneli.Event) {
		switch e.(type) {
		case goneli.LeaderAcquired:
			h.onAcquired()
		case goneli.LeaderRevoked:
			h.onRevoked()
		case goneli.LeaderFenced:
			h.onFenced()
		}
	})
	if err != nil {
		return err
	}
	h.neli = n

	h.throughput = metric.NewMeter("throughput", *h.config.Limits.MinMetricsInterval)

	h.state.Set(Running)
	go backgroundPoller(h)
	return nil
}

// IsLeader returns true if the current Harvest is the leader among competing instances.
func (h *harvest) IsLeader() bool {
	return h.LeaderID() != nil
}

// LeaderID returns the leader UUID of the current instance, if it is a leader at the time of this call.
// Otherwise, a nil is returned.
func (h *harvest) LeaderID() *uuid.UUID {
	if stored := h.leaderID.Load().(uuid.UUID); stored != noLeader {
		return &stored
	}
	return nil
}

// InFlightRecords returns the number of in-flight records; i.e. records that have been published on Kafka for which an
// acknowledgement is still pending.
func (h *harvest) InFlightRecords() int {
	return h.inFlightRecords.GetInt()
}

// InFlightRecordKeys returns the keys of records that are still in-flight. For any given key, there will be at most one
// record pending acknowledgement.
func (h *harvest) InFlightRecordKeys() []string {
	view := h.inFlightKeys.View()
	keys := make([]string, len(view))

	i := 0
	for k := range view {
		keys[i] = k
		i++
	}
	return keys
}

// SetEventHandler assigns an optional event handler callback to be notified of changes in leader state as well as other
// events of interest.
//
// This method must be invoked prior to Start().
func (h *harvest) SetEventHandler(eventHandler EventHandler) {
	ensureState(h.State() == Created, "Cannot set event handler at this time")
	h.eventHandler = eventHandler
}

func (h *harvest) shouldBeRunning() bool {
	return h.shouldBeRunningFlag.Get() == 1
}

func (h *harvest) reportPanic(goroutineName string) {
	if r := recover(); r != nil {
		h.logger().E()("Caught panic in %s: %v", goroutineName, r)
		h.panicCause.Store(tracedPanic{r, string(debug.Stack())})
		h.logger().E()(string(debug.Stack()))
		h.Stop()
	}
}

func ensureState(expected bool, format string, args ...interface{}) {
	if !expected {
		panic(fmt.Errorf("state assertion failed: "+format, args...))
	}
}

func backgroundPoller(h *harvest) {
	h.logger().I()("Starting background poller")
	defer h.logger().I()("Stopped")
	defer h.state.Set(Stopped)
	defer h.reportPanic("background poller")
	defer h.db.Dispose()
	defer h.neli.Close()
	defer h.shutdownSendBattery()
	defer h.state.Set(Stopping)
	defer h.logger().I()("Stopping")

	for h.shouldBeRunning() {
		isLeader, err := h.neli.Pulse(1 * time.Millisecond)
		if err != nil {
			panic(err)
		}

		if isLeader {
			if h.forceRemarkFlag.Get() == 1 {
				h.logger().D()("Remark requested")
				h.shutdownSendBattery()
				h.refreshLeader()
			}
			if h.sendBattery == nil {
				inFlightRecordsValue := h.inFlightRecords.Get()
				ensureState(inFlightRecordsValue == 0, "inFlightRecords=%d", inFlightRecordsValue)
				inFlightKeysView := h.inFlightKeys.View()
				ensureState(len(inFlightKeysView) == 0, "inFlightKeys=%d", inFlightKeysView)
				h.spawnSendBattery()
			}
			onLeaderPoll(h)
		}
	}
}

func (h *harvest) spawnSendBattery() {
	ensureState(h.sendBattery == nil, "send battery not nil before spawn")
	h.logger().D()("Spawning send battery")
	h.sendBattery = newConcurrentBattery(*h.config.Limits.SendConcurrency, *h.config.Limits.SendBuffer, func(records chan OutboxRecord) {
		defer h.reportPanic("send cell")

		h.logger().T()("Creating Kafka producer with config %v", h.producerConfigs)
		prod, err := h.config.KafkaProducerProvider(&h.producerConfigs)
		if err != nil {
			panic(err)
		}

		deliveryHandlerDone := make(chan int)
		go backgroundDeliveryHandler(h, prod, deliveryHandlerDone)

		defer func() {
			<-deliveryHandlerDone
		}()
		defer func() {
			go func() {
				// A bug in confluent-kafka-go occasionally causes an indefinite syscall hang in Close(), after it closes
				// the Events channel. So we delegate this to a separate goroutine — better an orphaned goroutine than a
				// frozen harvester. (The rest of the battery will still unwind normally.)
				closeWatcher := h.watch("close producer")
				prod.Close()
				closeWatcher.End()
			}()
		}()

		var lastID *int64
		for rec := range records {
			ensureState(lastID == nil || rec.ID >= *lastID, "discontinuity for key %s: ID %s, lastID: %v", rec.KafkaKey, rec.ID, lastID)
			lastID = &rec.ID

			m := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &rec.KafkaTopic, Partition: kafka.PartitionAny},
				Key:            []byte(rec.KafkaKey),
				Value:          stringPointerToByteArray(rec.KafkaValue),
				Opaque:         rec,
				Headers:        toNativeKafkaHeaders(rec.KafkaHeaders),
			}

			h.inFlightRecords.Drain(int64(*h.config.Limits.MaxInFlightRecords-1), concurrent.Indefinitely)

			startTime := time.Now()
			for {
				if h.deadlineExceeded("poll", h.neli.Deadline().Elapsed(), *h.config.Limits.MaxPollInterval) {
					break
				}
				if h.deadlineExceeded("message queueing", time.Now().Sub(startTime), *h.config.Limits.QueueTimeout) {
					break
				}
				if remaining := h.inFlightKeys.Drain(rec.KafkaKey, 0, *h.config.Limits.DrainInterval); remaining <= 0 {
					ensureState(remaining == 0, "drain failed: %d remaining in-flight records for key %s", remaining, rec.KafkaKey)
					break
				}
				h.logger().D()("Drain stalled for record %d (key %s)", rec.ID, rec.KafkaKey)
			}

			if h.forceRemarkFlag.Get() == 1 {
				h.queuedRecords.Dec()
				continue
			}

			h.inFlightRecords.Inc()
			h.queuedRecords.Dec()
			h.inFlightKeys.Inc(rec.KafkaKey)

			err := prod.Produce(m, nil)
			if err != nil {
				h.logger().W()("Error publishing record %v: %v", rec, err)
				h.inFlightKeys.Dec(rec.KafkaKey)
				h.inFlightRecords.Dec()
				h.forceRemarkFlag.Set(1)
			}
		}
	})
}

func stringPointerToByteArray(str *string) []byte {
	if str != nil {
		return []byte(*str)
	}
	return nil
}

func (h *harvest) shutdownSendBattery() {
	if h.sendBattery != nil {
		shutdownWatcher := h.watch("shutdown send battery")
		h.logger().D()("Shutting down send battery")

		// Expedite shutdown by raising the remark flag, forcing any queued records to be skipped.
		h.forceRemarkFlag.Set(1)

		// Take the battery down, waiting for all goroutines to complete.
		h.sendBattery.shutdown()
		h.sendBattery = nil

		// Reset flags and counters for next time.
		h.forceRemarkFlag.Set(0)
		h.inFlightRecords.Set(0)
		h.inFlightKeys.Clear()
		h.logger().D()("Send battery terminated")
		shutdownWatcher.End()
	}
}

func onLeaderPoll(h *harvest) {
	markBegin := time.Now()
	records, err := h.db.Mark(*h.LeaderID(), *h.config.Limits.MarkQueryRecords)

	if err != nil {
		h.logger().W()("Error executing mark query: %v", err)
		time.Sleep(*h.config.Limits.IOErrorBackoff)
		return
	}

	if len(records) > 0 {
		sendBegin := time.Now()
		h.logger().T()("Leader poll: marked %d starting with ID: %d, took %v", len(records), records[0].ID, sendBegin.Sub(markBegin))

		enqueueWatcher := h.watch("enqueue marked records")
		for _, rec := range records {
			h.queuedRecords.Inc()
			h.sendBattery.enqueue(rec)
		}
		enqueueWatcher.End()
		h.logger().T()("Send took %v", time.Now().Sub(sendBegin))
	} else {
		time.Sleep(*h.config.Limits.MarkBackoff)
	}
}

func (h *harvest) watch(operation string) *diags.Watcher {
	return diags.Watch(operation, watcherTimeout, diags.Print(h.logger().W()))
}

func (h *harvest) refreshLeader() {
	newLeaderID, _ := uuid.NewRandom()
	h.leaderID.Store(newLeaderID)
	h.logger().W()("Refreshed leader ID: %v", newLeaderID)
	h.eventHandler(LeaderRefreshed{newLeaderID})
}

func (h *harvest) deadlineExceeded(deadline string, elapsed time.Duration, threshold time.Duration) bool {
	if excess := elapsed - threshold; excess > 0 {
		if h.forceRemarkFlag.CompareAndSwap(0, 1) {
			h.logger().W()("Exceeded %s deadline by %v", deadline, excess)
		}
		return true
	}
	return false
}

func backgroundDeliveryHandler(h *harvest, prod KafkaProducer, done chan int) {
	h.logger().I()("Starting background delivery handler")
	defer h.reportPanic("background delivery handler")
	defer close(done)

	for e := range prod.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			rec := ev.Opaque.(OutboxRecord)
			if ev.TopicPartition.Error != nil {
				onFailedDelivery(h, rec, ev.TopicPartition.Error)
			} else {
				onSuccessfulDelivery(h, rec)
				h.updateStats()
			}
		default:
			h.logger().I()("Observed event: %v (%T)", e, e)
		}
	}
}

func (h *harvest) updateStats() {
	h.throughputLock.Lock()
	defer h.throughputLock.Unlock()
	h.throughput.MaybeStatsCall(func(stats metric.MeterStats) {
		h.logger().D()("%v", stats)
		h.eventHandler(MeterRead{stats})
	})
	h.throughput.Add(1)
}

func onSuccessfulDelivery(h *harvest, rec OutboxRecord) {
	for {
		done, err := h.db.Purge(rec.ID)
		if err == nil {
			if !done {
				h.logger().W()("Did not purge record %v", rec)
			}
			break
		}
		h.logger().W()("Error executing purge query for record %v: %v", rec, err)
		time.Sleep(*h.config.Limits.IOErrorBackoff)
	}
	h.inFlightKeys.Dec(rec.KafkaKey)
	h.inFlightRecords.Dec()
}

func onFailedDelivery(h *harvest, rec OutboxRecord, err error) {
	h.logger().W()("Delivery failed for %v, err: %v", rec, err)
	for {
		done, err := h.db.Reset(rec.ID)
		if err == nil {
			if !done {
				h.logger().W()("Did not reset record %v", rec)
			} else {
				h.forceRemarkFlag.Set(1)
			}
			break
		}
		h.logger().W()("Error executing reset query for record %v: %v", rec, err)
		time.Sleep(*h.config.Limits.IOErrorBackoff)
	}
	h.inFlightKeys.Dec(rec.KafkaKey)
	h.inFlightRecords.Dec()
}

func (h *harvest) onAcquired() {
	newLeaderID, _ := uuid.NewRandom()
	h.leaderID.Store(newLeaderID)
	h.logger().I()("Elected as leader, ID: %v", newLeaderID)
	h.eventHandler(LeaderAcquired{newLeaderID})
}

func (h *harvest) onRevoked() {
	h.logger().I()("Lost leader status")
	h.cleanupLeaderState()
	h.eventHandler(LeaderRevoked{})
}

func (h *harvest) onFenced() {
	h.logger().W()("Leader fenced")
	h.cleanupLeaderState()
	h.eventHandler(LeaderFenced{})
}

func (h *harvest) cleanupLeaderState() {
	h.shutdownSendBattery()
	h.leaderID.Store(noLeader)
}

// Stop the harvester, returning immediately.
//
// This method does not wait until the underlying Goroutines have been terminated
// and all resources have been disposed off properly. This is accomplished by calling Await()
func (h *harvest) Stop() {
	h.shouldBeRunningFlag.Set(0)
}

// Await the termination of this Harvest instance.
//
// This method blocks indefinitely, returning only when this instance has completed an orderly shutdown. I.e.
// when all Goroutines have returned and all resources have been disposed of.
func (h *harvest) Await() error {
	h.state.Await(concurrent.RefEqual(Stopped), concurrent.Indefinitely)
	panicCause := h.panicCause.Load()
	if panicCause != nil {
		return panicCause.(tracedPanic)
	}
	return nil
}
