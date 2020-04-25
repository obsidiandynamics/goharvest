package goharvest

import (
	"fmt"
	"os"
	"time"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/obsidiandynamics/goneli"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"gopkg.in/yaml.v2"
)

// Duration is a convenience for deriving a pointer from a given Duration argument.
func Duration(d time.Duration) *time.Duration {
	return &d
}

// Int is a convenience for deriving a pointer from a given int argument.
func Int(i int) *int {
	return &i
}

// Limits configuration.
type Limits struct {
	IOErrorBackoff     *time.Duration `yaml:"ioErrorBackoff"`
	PollDuration       *time.Duration `yaml:"pollDuration"`
	MinPollInterval    *time.Duration `yaml:"minPollInterval"`
	MaxPollInterval    *time.Duration `yaml:"maxPollInterval"`
	HeartbeatTimeout   *time.Duration `yaml:"heartbeatTimeout"`
	DrainInterval      *time.Duration `yaml:"drainInterval"`
	QueueTimeout       *time.Duration `yaml:"queueTimeout"`
	MarkBackoff        *time.Duration `yaml:"markBackoff"`
	MaxInFlightRecords *int           `yaml:"maxInFlightRecords"`
	SendConcurrency    *int           `yaml:"sendConcurrency"`
	SendBuffer         *int           `yaml:"sendBuffer"`
	MarkQueryRecords   *int           `yaml:"markQueryRecords"`
	MinMetricsInterval *time.Duration `yaml:"minMetricsInterval"`
}

func defaultInt(i **int, def int) {
	if *i == nil {
		*i = &def
	}
}

func defaultDuration(d **time.Duration, def time.Duration) {
	if *d == nil {
		*d = &def
	}
}

// SetDefaults assigns the defaults for optional values.
func (l *Limits) SetDefaults() {
	defaultDuration(&l.IOErrorBackoff, 500*time.Millisecond)
	defaultDuration(&l.HeartbeatTimeout, goneli.DefaultHeartbeatTimeout)
	defaultDuration(&l.MaxPollInterval, *l.HeartbeatTimeout/2)
	defaultDuration(&l.QueueTimeout, 30*time.Second)
	defaultDuration(&l.DrainInterval, minDuration(*l.MaxPollInterval, *l.QueueTimeout))
	defaultDuration(&l.MarkBackoff, 10*time.Millisecond)
	defaultInt(&l.MaxInFlightRecords, 1000)
	defaultInt(&l.SendConcurrency, 8)
	defaultInt(&l.SendBuffer, 10)
	defaultInt(&l.MarkQueryRecords, 100)
	defaultDuration(&l.MinMetricsInterval, 5*time.Second)
}

func minDuration(d0, d1 time.Duration) time.Duration {
	if d0 < d1 {
		return d0
	}
	return d1
}

// Validate the Limits configuration, returning an error if invalid
func (l Limits) Validate() error {
	minimumMaxPollInterval := 1 * time.Millisecond
	if l.MinPollInterval != nil {
		minimumMaxPollInterval = *l.MinPollInterval
	}
	return validation.ValidateStruct(&l,
		validation.Field(&l.IOErrorBackoff, validation.Min(0)),
		validation.Field(&l.DrainInterval, validation.Required, validation.Min(1*time.Millisecond)),
		validation.Field(&l.MaxPollInterval, validation.Required, validation.Min(minimumMaxPollInterval)),
		validation.Field(&l.QueueTimeout, validation.Required, validation.Min(1*time.Millisecond)),
		validation.Field(&l.MarkBackoff, validation.Min(0)),
		validation.Field(&l.MaxInFlightRecords, validation.Required, validation.Min(1)),
		validation.Field(&l.SendConcurrency, validation.Required, validation.Min(1)),
		validation.Field(&l.SendBuffer, validation.Min(0)),
		validation.Field(&l.MarkQueryRecords, validation.Required, validation.Min(1)),
		validation.Field(&l.MinMetricsInterval, validation.Min(0)),
	)
}

// String obtains a textural representation of Limits.
func (l Limits) String() string {
	return fmt.Sprint(
		"Limits[IOErrorBackoff=", l.IOErrorBackoff,
		", PollDuration=", l.PollDuration,
		", MinPollInterval=", l.MinPollInterval,
		", MaxPollInterval=", l.MaxPollInterval,
		", HeartbeatTimeout=", l.HeartbeatTimeout,
		", DrainInterval=", l.DrainInterval,
		", QueueTimeout=", l.QueueTimeout,
		", MarkBackoff=", l.MarkBackoff,
		", MaxInFlightRecords=", l.MaxInFlightRecords,
		", SendConcurrency=", l.SendConcurrency,
		", SendBuffer=", l.SendBuffer,
		", MarkQueryRecords=", l.MarkQueryRecords,
		", MinMetricsInterval=", l.MinMetricsInterval, "]",
	)
}

// KafkaConfigMap represents the Kafka key-value configuration.
type KafkaConfigMap map[string]interface{}

// Config encapsulates configuration for Harvest.
type Config struct {
	BaseKafkaConfig         KafkaConfigMap `yaml:"baseKafkaConfig"`
	ProducerKafkaConfig     KafkaConfigMap `yaml:"producerKafkaConfig"`
	LeaderTopic             string         `yaml:"leaderTopic"`
	LeaderGroupID           string         `yaml:"leaderGroupID"`
	DataSource              string         `yaml:"dataSource"`
	OutboxTable             string         `yaml:"outboxTable"`
	Limits                  Limits         `yaml:"limits"`
	KafkaConsumerProvider   KafkaConsumerProvider
	KafkaProducerProvider   KafkaProducerProvider
	DatabaseBindingProvider DatabaseBindingProvider
	NeliProvider            NeliProvider
	Scribe                  scribe.Scribe
	Name                    string `yaml:"name"`
}

// Validate the Config, returning an error if invalid.
func (c Config) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.BaseKafkaConfig, validation.NotNil),
		validation.Field(&c.ProducerKafkaConfig, validation.NotNil),
		validation.Field(&c.DataSource, validation.Required),
		validation.Field(&c.OutboxTable, validation.Required),
		validation.Field(&c.Limits),
		validation.Field(&c.KafkaConsumerProvider, validation.NotNil),
		validation.Field(&c.KafkaProducerProvider, validation.NotNil),
		validation.Field(&c.DatabaseBindingProvider, validation.NotNil),
		validation.Field(&c.NeliProvider, validation.NotNil),
		validation.Field(&c.Scribe, validation.NotNil),
		validation.Field(&c.Name, validation.Required),
	)
}

// Obtains a textual representation of the configuration.
func (c Config) String() string {
	return fmt.Sprint(
		"Config[BaseKafkaConfig=", c.BaseKafkaConfig,
		", ProducerKafkaConfig=", c.ProducerKafkaConfig,
		", LeaderTopic=", c.LeaderTopic,
		", LeaderGroupID=", c.LeaderGroupID,
		", DataSource=", c.DataSource,
		", OutboxTable=", c.OutboxTable,
		", Limits=", c.Limits,
		", KafkaConsumerProvider=", c.KafkaConsumerProvider,
		", KafkaProducerProvider=", c.KafkaProducerProvider,
		", DatabaseBindingProvider=", c.DatabaseBindingProvider,
		", NeliProvider=", c.NeliProvider,
		", Scribe=", c.Scribe,
		", Name=", c.Name, "]")
}

// SetDefaults assigns the default values to optional fields.
func (c *Config) SetDefaults() {
	if c.BaseKafkaConfig == nil {
		c.BaseKafkaConfig = KafkaConfigMap{}
	}
	if _, ok := c.BaseKafkaConfig["bootstrap.servers"]; !ok {
		c.BaseKafkaConfig["bootstrap.servers"] = "localhost:9092"
	}
	if c.ProducerKafkaConfig == nil {
		c.ProducerKafkaConfig = KafkaConfigMap{}
	}
	if c.DataSource == "" {
		c.DataSource = "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable"
	}
	if c.OutboxTable == "" {
		c.OutboxTable = "outbox"
	}
	c.Limits.SetDefaults()
	if c.KafkaConsumerProvider == nil {
		c.KafkaConsumerProvider = StandardKafkaConsumerProvider()
	}
	if c.KafkaProducerProvider == nil {
		c.KafkaProducerProvider = StandardKafkaProducerProvider()
	}
	if c.DatabaseBindingProvider == nil {
		c.DatabaseBindingProvider = StandardPostgresBindingProvider()
	}
	if c.NeliProvider == nil {
		c.NeliProvider = StandardNeliProvider()
	}
	if c.Scribe == nil {
		c.Scribe = scribe.New(scribe.StandardBinding())
	}
	if c.Name == "" {
		c.Name = fmt.Sprintf("%s_%d_%d", goneli.Sanitise(getString("localhost", os.Hostname)), os.Getpid(), time.Now().Unix())
	}
}

// Unmarshal a configuration from a byte slice, returning the configuration struct with pre-initialised defaults,
// or an error if unmarshalling failed. The configuration is not validated prior to returning, in case further
// amendments are required by the caller. The caller should call Validate() independently.
func Unmarshal(in []byte) (Config, error) {
	cfg := Config{}
	err := yaml.UnmarshalStrict(in, &cfg)
	if err == nil {
		cfg.SetDefaults()
	}
	return cfg, err
}

type stringGetter func() (string, error)

func getString(def string, stringGetter stringGetter) string {
	str, err := stringGetter()
	if err != nil {
		return def
	}
	return str
}
