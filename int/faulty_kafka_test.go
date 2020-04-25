package int

import (
	"github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/libstdgo/fault"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type ProducerFaultSpecs struct {
	OnProduce  fault.Spec
	OnDelivery fault.Spec
}

func (specs ProducerFaultSpecs) build() producerFaults {
	return producerFaults{
		onProduce:  specs.OnProduce.Build(),
		onDelivery: specs.OnDelivery.Build(),
	}
}

func FaultyKafkaProducerProvider(realProvider goharvest.KafkaProducerProvider, specs ProducerFaultSpecs) goharvest.KafkaProducerProvider {
	return func(conf *goharvest.KafkaConfigMap) (goharvest.KafkaProducer, error) {
		real, err := realProvider(conf)
		if err != nil {
			return nil, err
		}
		return newFaultyProducer(real, specs.build()), nil
	}
}

type producerFaults struct {
	onProduce  fault.Fault
	onDelivery fault.Fault
}

type faultyProducer struct {
	real   goharvest.KafkaProducer
	faults producerFaults
	events chan kafka.Event
}

func newFaultyProducer(real goharvest.KafkaProducer, faults producerFaults) *faultyProducer {
	f := &faultyProducer{
		real:   real,
		faults: faults,
		events: make(chan kafka.Event),
	}

	go func() {
		defer close(f.events)

		for e := range real.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					f.events <- e
				} else if err := f.faults.onDelivery.Try(); err != nil {
					rewrittenMessage := *ev
					rewrittenMessage.TopicPartition = kafka.TopicPartition{
						Topic:     ev.TopicPartition.Topic,
						Partition: ev.TopicPartition.Partition,
						Offset:    ev.TopicPartition.Offset,
						Metadata:  ev.TopicPartition.Metadata,
						Error:     err,
					}
					f.events <- &rewrittenMessage
				} else {
					f.events <- e
				}
			default:
				f.events <- e
			}
		}
	}()

	return f
}

func (f *faultyProducer) Events() chan kafka.Event {
	return f.events
}

func (f *faultyProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if err := f.faults.onProduce.Try(); err != nil {
		return err
	}
	return f.real.Produce(msg, deliveryChan)
}

func (f *faultyProducer) Close() {
	f.real.Close()
}
