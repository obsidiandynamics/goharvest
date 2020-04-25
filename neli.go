package goharvest

import "github.com/obsidiandynamics/goneli"

// NeliProvider is a factory for creating Neli instances.
type NeliProvider func(config goneli.Config, barrier goneli.Barrier) (goneli.Neli, error)

// StandardNeliProvider returns a factory for creating a conventional Neli instance, backed by the real client API.
func StandardNeliProvider() NeliProvider {
	return func(config goneli.Config, barrier goneli.Barrier) (goneli.Neli, error) {
		return goneli.New(config, barrier)
	}
}

func configToNeli(hConfigMap KafkaConfigMap) goneli.KafkaConfigMap {
	return map[string]interface{}(hConfigMap)
}

func configToHarvest(nConfigMap goneli.KafkaConfigMap) KafkaConfigMap {
	return map[string]interface{}(nConfigMap)
}

func convertKafkaConsumerProvider(hProvider KafkaConsumerProvider) goneli.KafkaConsumerProvider {
	return func(conf *goneli.KafkaConfigMap) (goneli.KafkaConsumer, error) {
		hCfg := configToHarvest(*conf)
		return hProvider(&hCfg)
	}
}

func convertKafkaProducerProvider(hProvider KafkaProducerProvider) goneli.KafkaProducerProvider {
	return func(conf *goneli.KafkaConfigMap) (goneli.KafkaProducer, error) {
		hCfg := configToHarvest(*conf)
		return hProvider(&hCfg)
	}
}
