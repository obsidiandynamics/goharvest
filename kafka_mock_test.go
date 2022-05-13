package goharvest

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/obsidiandynamics/libstdgo/concurrent"
)

type consMockFuncs struct {
	Subscribe   func(m *consMock, topic string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage func(m *consMock, timeout time.Duration) (*kafka.Message, error)
	Close       func(m *consMock) error
}

type consMockCounts struct {
	Subscribe,
	ReadMessage,
	Close concurrent.AtomicCounter
}

type consMock struct {
	rebalanceCallback kafka.RebalanceCb
	rebalanceEvents   chan kafka.Event
	f                 consMockFuncs
	c                 consMockCounts
}

func (m *consMock) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	defer m.c.Subscribe.Inc()
	m.rebalanceCallback = rebalanceCb
	return m.f.Subscribe(m, topic, rebalanceCb)
}

func (m *consMock) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	defer m.c.ReadMessage.Inc()
	if m.rebalanceCallback != nil {
		// The rebalance events should only be delivered in the polling thread, which is why we wait for
		// ReadMessage before forwarding the events to the rebalance callback
		select {
		case e := <-m.rebalanceEvents:
			m.rebalanceCallback(nil, e)
		default:
		}
	}
	return m.f.ReadMessage(m, timeout)
}

func (m *consMock) Close() error {
	defer m.c.Close.Inc()
	return m.f.Close(m)
}

func (m *consMock) fillDefaults() {
	if m.rebalanceEvents == nil {
		m.rebalanceEvents = make(chan kafka.Event)
	}
	if m.f.Subscribe == nil {
		m.f.Subscribe = func(m *consMock, topic string, rebalanceCb kafka.RebalanceCb) error {
			return nil
		}
	}
	if m.f.ReadMessage == nil {
		m.f.ReadMessage = func(m *consMock, timeout time.Duration) (*kafka.Message, error) {
			return nil, newTimedOutError()
		}
	}
	if m.f.Close == nil {
		m.f.Close = func(m *consMock) error {
			return nil
		}
	}
	m.c.Subscribe = concurrent.NewAtomicCounter()
	m.c.ReadMessage = concurrent.NewAtomicCounter()
	m.c.Close = concurrent.NewAtomicCounter()
}

func mockKafkaConsumerProvider(m *consMock) func(conf *KafkaConfigMap) (KafkaConsumer, error) {
	return func(conf *KafkaConfigMap) (KafkaConsumer, error) {
		return m, nil
	}
}

type prodMockFuncs struct {
	Events  func(m *prodMock) chan kafka.Event
	Produce func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close   func(m *prodMock)
}

type prodMockCounts struct {
	Events,
	Produce,
	Close concurrent.AtomicCounter
}

type prodMock struct {
	events chan kafka.Event
	f      prodMockFuncs
	c      prodMockCounts
}

func (m *prodMock) Events() chan kafka.Event {
	defer m.c.Events.Inc()
	return m.f.Events(m)
}

func (m *prodMock) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	defer m.c.Produce.Inc()
	return m.f.Produce(m, msg, deliveryChan)
}

func (m *prodMock) Close() {
	defer m.c.Close.Inc()
	m.f.Close(m)
}

func (m *prodMock) fillDefaults() {
	if m.events == nil {
		m.events = make(chan kafka.Event)
	}
	if m.f.Events == nil {
		m.f.Events = func(m *prodMock) chan kafka.Event {
			return m.events
		}
	}
	if m.f.Produce == nil {
		m.f.Produce = func(m *prodMock, msg *kafka.Message, deliveryChan chan kafka.Event) error {
			return nil
		}
	}
	if m.f.Close == nil {
		m.f.Close = func(m *prodMock) {
			close(m.events)
		}
	}
	m.c.Events = concurrent.NewAtomicCounter()
	m.c.Produce = concurrent.NewAtomicCounter()
	m.c.Close = concurrent.NewAtomicCounter()
}

func mockKafkaProducerProvider(m *prodMock) func(conf *KafkaConfigMap) (KafkaProducer, error) {
	return func(conf *KafkaConfigMap) (KafkaProducer, error) {
		return m, nil
	}
}
