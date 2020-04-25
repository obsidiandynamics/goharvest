package goharvest

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/obsidiandynamics/goneli"
	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDefaultKafkaConsumerProvider(t *testing.T) {
	c := Config{}
	c.SetDefaults()

	cons, err := c.KafkaConsumerProvider(&KafkaConfigMap{})
	assert.Nil(t, cons)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "Required property")
	}
}

func TestDefaultKafkaProducerProvider(t *testing.T) {
	c := Config{}
	c.SetDefaults()

	prod, err := c.KafkaProducerProvider(&KafkaConfigMap{"foo": "bar"})
	assert.Nil(t, prod)
	if assert.NotNil(t, err) {
		assert.Contains(t, err.Error(), "No such configuration property")
	}
}

func TestDefaultNeliProvider(t *testing.T) {
	c := Config{}
	c.SetDefaults()

	consMock := &consMock{}
	consMock.fillDefaults()
	prodMock := &prodMock{}
	prodMock.fillDefaults()
	neli, err := c.NeliProvider(goneli.Config{
		KafkaConsumerProvider: convertKafkaConsumerProvider(mockKafkaConsumerProvider(consMock)),
		KafkaProducerProvider: convertKafkaProducerProvider(mockKafkaProducerProvider(prodMock)),
	}, goneli.NopBarrier())
	assert.NotNil(t, neli)
	assert.Nil(t, err)
	assert.Nil(t, neli.Close())
}

func TestLimitsString(t *testing.T) {
	lim := Limits{}
	lim.SetDefaults()
	assert.Contains(t, lim.String(), "Limits[")
}

func TestLimitsFromYaml(t *testing.T) {
	const y = `
ioErrorBackoff: 10ms
pollDuration: 20ms
minPollInterval: 30ms
`
	lim := Limits{}
	err := yaml.UnmarshalStrict([]byte(y), &lim)
	assert.Nil(t, err)
	assert.Equal(t, 10*time.Millisecond, *lim.IOErrorBackoff)
	assert.Equal(t, 20*time.Millisecond, *lim.PollDuration)
	assert.Equal(t, 30*time.Millisecond, *lim.MinPollInterval)

	lim.SetDefaults()

	// Check that the defaults weren't overridden.
	def := Limits{}
	def.SetDefaults()
	assert.Equal(t, *def.MarkBackoff, *lim.MarkBackoff)
}

func TestGetString(t *testing.T) {
	assert.Equal(t, "some-default", getString("some-default", func() (string, error) { return "", check.ErrSimulated }))
	assert.Equal(t, "some-string", getString("some-default", func() (string, error) { return "some-string", nil }))
}

func TestValidateLimits(t *testing.T) {
	lim := Limits{}
	lim.SetDefaults()
	assert.Nil(t, lim.Validate())

	lim = Limits{
		IOErrorBackoff: Duration(-1),
		PollDuration:   Duration(time.Millisecond),
	}
	lim.SetDefaults()
	if err := lim.Validate(); assert.NotNil(t, err) {
		assert.Equal(t, "IOErrorBackoff: must be no less than 0.", lim.Validate().Error())
	}

	lim = Limits{
		DrainInterval: Duration(0),
	}
	lim.SetDefaults()
	if err := lim.Validate(); assert.NotNil(t, err) {
		assert.Equal(t, "DrainInterval: cannot be blank.", lim.Validate().Error())
	}

	lim = Limits{
		DrainInterval: Duration(1 * time.Nanosecond),
	}
	lim.SetDefaults()
	if err := lim.Validate(); assert.NotNil(t, err) {
		assert.Equal(t, "DrainInterval: must be no less than 1ms.", lim.Validate().Error())
	}
}

func TestConfigString(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	assert.Contains(t, cfg.String(), "Config[")
}

func TestValidateConfig_valid(t *testing.T) {
	cfg := Config{
		BaseKafkaConfig:         KafkaConfigMap{},
		ProducerKafkaConfig:     KafkaConfigMap{},
		LeaderTopic:             "leader-topic",
		LeaderGroupID:           "leader-group-d",
		DataSource:              "data-source",
		OutboxTable:             "outbox-table",
		KafkaConsumerProvider:   StandardKafkaConsumerProvider(),
		KafkaProducerProvider:   StandardKafkaProducerProvider(),
		DatabaseBindingProvider: StandardPostgresBindingProvider(),
		Scribe:                  scribe.New(scribe.StandardBinding()),
		Name:                    "name",
	}
	cfg.SetDefaults()
	assert.Nil(t, cfg.Validate())
}

func TestValidateConfig_invalidLimits(t *testing.T) {
	cfg := Config{
		BaseKafkaConfig:     KafkaConfigMap{},
		ProducerKafkaConfig: KafkaConfigMap{},
		LeaderTopic:         "leader-topic",
		LeaderGroupID:       "leader-group-id",
		DataSource:          "data-source",
		OutboxTable:         "outbox-table",
		Limits: Limits{
			SendConcurrency: Int(-1),
		},
		KafkaConsumerProvider:   StandardKafkaConsumerProvider(),
		KafkaProducerProvider:   StandardKafkaProducerProvider(),
		DatabaseBindingProvider: StandardPostgresBindingProvider(),
		Scribe:                  scribe.New(scribe.StandardBinding()),
		Name:                    "name",
	}
	cfg.SetDefaults()
	assert.NotNil(t, cfg.Validate())
}

func TestValidateConfig_default(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaults()
	assert.Nil(t, cfg.Validate())
}

func TestDefaultDrainTimeout(t *testing.T) {
	cfg := Config{
		Limits: Limits{
			HeartbeatTimeout: Duration(40 * time.Second),
		},
	}
	cfg.SetDefaults()
	assert.Equal(t, 20*time.Second, *cfg.Limits.MaxPollInterval)
	assert.Equal(t, 20*time.Second, *cfg.Limits.DrainInterval)

	cfg = Config{
		Limits: Limits{
			HeartbeatTimeout: Duration(40 * time.Second),
			QueueTimeout:     Duration(15 * time.Second),
		},
	}
	cfg.SetDefaults()
	assert.Equal(t, 20*time.Second, *cfg.Limits.MaxPollInterval)
	assert.Equal(t, 15*time.Second, *cfg.Limits.DrainInterval)
}

func TestUnmarshal_fullyPopulated(t *testing.T) {
	const y = `
baseKafkaConfig: 
  bootstrap.servers: localhost:9093
producerKafkaConfig:
  compression.type: lz4
leaderTopic: leader-topic
leaderGroupID: leader-group-id
dataSource: data-source
outboxTable: outbox-table
limits:
  ioErrorBackoff: 10ms
  pollDuration: 20ms
  minPollInterval: 30ms
  maxPollInterval: 35ms
  heartbeatTimeout: 15ms	
  drainInterval: 32ms
  queueTimeout: 40ms	
  markBackoff: 50ms
  maxInFlightRecords: 60
  sendConcurrency: 70
  sendBuffer: 80
  minMetricsInterval: 90ms
name: test-name
`
	cfg, err := Unmarshal([]byte(y))
	require.Nil(t, err)
	if !assert.Nil(t, cfg.Validate()) {
		t.Errorf("Validation error: %s", cfg.Validate().Error())
	}
	exp := Config{
		BaseKafkaConfig: KafkaConfigMap{
			"bootstrap.servers": "localhost:9093",
		},
		ProducerKafkaConfig: KafkaConfigMap{
			"compression.type": "lz4",
		},
		LeaderTopic:   "leader-topic",
		LeaderGroupID: "leader-group-id",
		DataSource:    "data-source",
		OutboxTable:   "outbox-table",
		Limits: Limits{
			IOErrorBackoff:     Duration(10 * time.Millisecond),
			PollDuration:       Duration(20 * time.Millisecond),
			MinPollInterval:    Duration(30 * time.Millisecond),
			MaxPollInterval:    Duration(35 * time.Millisecond),
			HeartbeatTimeout:   Duration(15 * time.Millisecond),
			DrainInterval:      Duration(32 * time.Millisecond),
			QueueTimeout:       Duration(40 * time.Millisecond),
			MarkBackoff:        Duration(50 * time.Millisecond),
			MaxInFlightRecords: Int(60),
			SendConcurrency:    Int(70),
			SendBuffer:         Int(80),
			MinMetricsInterval: Duration(90 * time.Millisecond),
		},
		Name: "test-name",
	}
	exp.SetDefaults()
	ignoreFields := cmpopts.IgnoreFields(
		Config{},
		"KafkaConsumerProvider", "KafkaProducerProvider", "DatabaseBindingProvider", "NeliProvider", "Scribe",
	)
	assert.True(t, cmp.Equal(exp, cfg, ignoreFields), "Diff: %v", cmp.Diff(exp, cfg, ignoreFields))
}

func TestUnmarshal_empty(t *testing.T) {
	const y = ``
	cfg, err := Unmarshal([]byte(y))
	assert.Nil(t, err)
	if !assert.Nil(t, cfg.Validate()) {
		t.Errorf("Validation error: %s", cfg.Validate().Error())
	}
	exp := Config{}
	exp.SetDefaults()
	ignoreFields := cmpopts.IgnoreFields(
		Config{},
		"KafkaConsumerProvider", "KafkaProducerProvider", "DatabaseBindingProvider", "NeliProvider", "Scribe", "Name",
	)
	assert.True(t, cmp.Equal(exp, cfg, ignoreFields), "Diff: %v", cmp.Diff(exp, cfg, ignoreFields))
}
