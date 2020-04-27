<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goharvest/images/goharvest-logo-wide.png" width="400px" alt="logo"/>&nbsp;
===
![Go version](https://img.shields.io/github/go-mod/go-version/obsidiandynamics/goharvest)
[![Build](https://travis-ci.org/obsidiandynamics/goharvest.svg?branch=master) ](https://travis-ci.org/obsidiandynamics/goharvest#)
![Release](https://img.shields.io/github/v/release/obsidiandynamics/goharvest?color=ff69b4)
[![Codecov](https://codecov.io/gh/obsidiandynamics/goharvest/branch/master/graph/badge.svg)](https://codecov.io/gh/obsidiandynamics/goharvest)
[![Go Report Card](https://goreportcard.com/badge/github.com/obsidiandynamics/goharvest)](https://goreportcard.com/report/github.com/obsidiandynamics/goharvest)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/obsidiandynamics/goharvest.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/obsidiandynamics/goharvest/alerts/)
[![GoDoc Reference](https://img.shields.io/badge/docs-GoDoc-blue.svg)](https://pkg.go.dev/github.com/obsidiandynamics/goharvest?tab=doc)

`goharvest` is a Go implementation of the [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern for Postgres and Kafka.

<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goharvest/images/figure-outbox.png" width="100%" alt="Transactional Outbox"/>

While `goharvest` is a complex beast, the end result is dead simple: to publish Kafka messages reliably and atomically, simply write a record to a dedicated **outbox table** in a transaction, alongside any other database changes. (Outbox schema provided below.) `goharvest` scrapes the outbox table in the background and publishes records to a Kafka topic of the application's choosing, using the key, value and headers specified in the outbox record. `goharvest` currently works with Postgres. It maintains causal order of messages and does not require CDC to be enabled on the database, making for a zero-hassle setup. It handles thousands of records/second on commodity hardware.

# Getting started
## 1. Create an outbox table for your application
```sql
CREATE TABLE IF NOT EXISTS outbox (
  id                  BIGSERIAL PRIMARY KEY,
  create_time         TIMESTAMP WITH TIME ZONE NOT NULL,
  kafka_topic         VARCHAR(249) NOT NULL,
  kafka_key           VARCHAR(100) NOT NULL,  -- pick your own maximum key size
  kafka_value         VARCHAR(10000),         -- pick your own maximum value size
  kafka_header_keys   TEXT[] NOT NULL,
  kafka_header_values TEXT[] NOT NULL,
  leader_id           UUID
)
```

## 2. Run `goharvest`
### Standalone mode
This runs `goharvest` within a separate process called `reaper`, which will work alongside **any** application that writes to a standard outbox. (Not just applications written in Go.)

#### Install `reaper`
```sh
go get -u github.com/obsidiandynamics/goharvest/cmd/reaper
```

#### Create `reaper.yaml` configuration
```yaml
harvest:
  baseKafkaConfig: 
    bootstrap.servers: localhost:9092
  producerKafkaConfig:
    compression.type: lz4
    delivery.timeout.ms: 10000
  leaderTopic: my-app-name
  leaderGroupID: my-app-name
  dataSource: host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable
  outboxTable: outbox
  limits:
    minPollInterval: 1s
    heartbeatTimeout: 5s
    maxInFlightRecords: 1000
    minMetricsInterval: 5s
    sendConcurrency: 4
    sendBuffer: 10
logging:
  level: Debug
```

#### Start `reaper`
```sh
reaper -f reaper.yaml
```

### Embedded mode
`goharvest` can be run in the same process as your application.

#### Add the dependency
```sh
go get -u github.com/obsidiandynamics/goharvest
```

#### Create and start a `Harvest` instance
```go
import "github.com/obsidiandynamics/goharvest"
```

```go
// Configure the harvester. It will use its own database and Kafka connections under the hood.
config := Config{
  BaseKafkaConfig: KafkaConfigMap{
    "bootstrap.servers": "localhost:9092",
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
```

### Using a custom logger
`goharvest` uses `log.Printf` for output by default. Logger configuration is courtesy of the Scribe façade, from [<code>libstdgo</code>](https://github.com/obsidiandynamics/libstdgo). The example below uses a Logrus binding for Scribe.

```go
import (
  "github.com/obsidiandynamics/goharvest"
  scribelogrus "github.com/obsidiandynamics/libstdgo/scribe/logrus"
  "github.com/sirupsen/logrus"
)
```

```sh
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
```

### Listening for leader status updates
Just like `goharvest` uses [NELI](https://github.com/obsidiandynamics/goneli) to piggy-back on Kafka's leader election, you can piggy-back on `goharvest` to get leader status updates:

```go
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
```

### Which mode should I use
Running `goharvest` in standalone mode using `reaper` is the recommended approach for most use cases, as it fully insulates the harvester from the rest of the application. Ideally, you should deploy `reaper` as a sidecar daemon, to run alongside your application. All the reaper needs is access to the outbox table and the Kafka cluster.

Embedded `goharvest` is useful if you require additional insights into its operation, which is accomplished by registering an `EventHandler` callback, as shown in the example above. This callback is invoked whenever the underlying leader status changes, which may be useful if you need to schedule additional workloads that should only be run on one process at any given time.

## 3. Write outbox records
### Directly, using SQL
You can write database records from any app, by simply issuing the following `INSERT` statement:

```sql
INSERT INTO ${outbox_table} (
  create_time, 
  kafka_topic, 
  kafka_key, 
  kafka_value, 
  kafka_header_keys, 
  kafka_header_values
)
VALUES (NOW(), $1, $2, $3, $4, $5)
```

Replace `${outbox_table}` and bind the query variables as appropriate:

* `kafka_topic` column specifies an arbitrary topic name, which may differ among records.
* `kafka_key` is a mandatory `string` key. Each record must be published with a specified key, which will affect its placement among the topic's partitions.
* `kafka_value` is an optional `string` value. If unspecified, the record will be published with a `nil` value, allowing it to be used as a compaction tombstone.
* `kafka_header_keys` and `kafka_header_values` are arrays that specify the keys and values of record headers. When used each element in `kafka_header_keys` corresponds to an element in `kafka_header_values` at the same index. If not using headers, set both arrays to empty.

> **Note**: **Writing outbox records should be performed in the same transaction as other related database updates.** Otherwise, messaging will not be atomic — the updates may be stably persisted while the message might be lost, and *vice versa*.

### Using `stasher`
The `goharvest` library comes with a `stasher` helper package for writing records to an outbox.

#### One-off messages
When one database update corresponds to one message, the easiest approach is to call `Stasher.Stash()`:

```go
import "github.com/obsidiandynamics/goharvest"
```

```go
db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable")
if err != nil {
  panic(err)
}
defer db.Close()

st := New("outbox")

// Begin a transaction.
tx, _ := db.Begin()
defer tx.Rollback()

// Update other database entities in transaction scope.

// Stash an outbox record for subsequent harvesting.
err = st.Stash(tx, goharvest.OutboxRecord{
  KafkaTopic: "my-app.topic",
  KafkaKey:   "hello",
  KafkaValue: goharvest.String("world"),
  KafkaHeaders: goharvest.KafkaHeaders{
    {Key: "applicationId", Value: "my-app"},
  },
})
if err != nil {
  panic(err)
}

// Commit the transaction.
tx.Commit()
```

#### Multiple messages
Sending multiple messages within a single transaction may be done more efficiently using prepared statements:

```go
// Begin a transaction.
tx, _ := db.Begin()
defer tx.Rollback()

// Update other database entities in transaction scope.
// ...

// Formulates a prepared statement that may be reused within the scope of the transaction.
prestash, _ := st.Prepare(tx)

// Publish a bunch of messages using the same prepared statement.
for i := 0; i < 10; i++ {
  // Stash an outbox record for subsequent harvesting.
  err = prestash.Stash(goharvest.OutboxRecord{
    KafkaTopic: "my-app.topic",
    KafkaKey:   "hello",
    KafkaValue: goharvest.String("world"),
    KafkaHeaders: goharvest.KafkaHeaders{
      {Key: "applicationId", Value: "my-app"},
    },
  })
  if err != nil {
    panic(err)
  }
}

// Commit the transaction.
tx.Commit()
```

# Configuration
There are handful of parameters that for configuring `goharvest`, assigned via the `Config` struct:

<table>
  <thead>
    <tr>
      <th>Parameter</th>
      <th>Default value</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code>BaseKafkaConfig</code></td>
      <td>Map containing <code>bootstrap.servers=localhost:9092</code>.</td>
      <td>Configuration shared by the underlying Kafka producer and consumer clients, including those used for leader election.</td>
    </tr>
    <tr valign="top">
      <td><code>ProducerKafkaConfig</code></td>
      <td>Empty map.</td>
      <td>Additional configuration on top of <code>BaseKafkaConfig</code> that is specific to the producer clients created by <code>goharvest</code> for publishing harvested messages. This configuration does not apply to the underlying NELI leader election protocol.</td>
    </tr>
    <tr valign="top">
      <td><code>LeaderGroupID</code></td>
      <td>Assumes the filename of the application binary.</td>
      <td>Used by the underlying leader election protocol as a unique identifier shared by all instances in a group of competing processes. The <code>LeaderGroupID</code> is used as Kafka <code>group.id</code> property under the hood, when subscribing to the leader election topic.</td>
    </tr>
    <tr valign="top">
      <td><code>LeaderTopic</code></td>
      <td>Assumes the value of <code>LeaderGroupID</code>, suffixed with the string <code>.neli</code>.</td>
      <td>Used by NELI as the name of the Kafka topic for orchestrating leader election. Competing processes subscribe to the same topic under an identical consumer group ID, using Kafka's exclusive partition assignment as a mechanism for arbitrating leader status.</td>
    </tr>
    <tr valign="top">
      <td><code>DataSource</code></td>
      <td>Local Postgres data source <code>host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable</code>.</td>
      <td>The database driver-specific data source string.</td>
    </tr>
    <tr valign="top">
      <td><code>OutboxTable</code></td>
      <td><code>outbox</code></td>
      <td>The name of the outbox table, optionally including the schema name.</td>
    </tr>
    <tr valign="top">
      <td><code>Scribe</code></td>
      <td>Scribe configured with bindings for <code>log.Printf()</code>; effectively the result of running <code>scribe.New(scribe.StandardBinding())</code>.</td>
      <td>The logging façade used by the library, preconfigured with your logger of choice. See <a href="https://pkg.go.dev/github.com/obsidiandynamics/libstdgo/scribe?tab=doc">Scribe GoDocs</a>.</td>
    </tr>
    <tr valign="top">
      <td><code>Name</code></td>
      <td>A string in the form <code>{hostname}_{pid}_{time}</code>, where <code>{hostname}</code> is the result of invoking <code>os.Hostname()</code>, <code>{pid}</code> is the process ID, and <code>{time}</code> is the UNIX epoch time, in seconds.</td>
      <td>The symbolic name of this instance. This field is informational only, accompanying all log messages.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.MinPollInterval</code></td>
      <td>100 ms</td>
      <td>The lower bound on the poll interval, preventing the over-polling of Kafka on successive <code>Pulse()</code> invocations. Assuming <code>Pulse()</code> is called repeatedly by the application, NELI may poll Kafka at a longer interval than <code>MinPollInterval</code>. (Regular polling is necessary to prove client's liveness and maintain internal partition assignment, but polling excessively is counterproductive.)</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.HeartbeatTimeout</code></td>
      <td>5 s</td>
      <td>The period that a leader will maintain its status, not having received a heartbeat message on the leader topic. After the timeout elapses, the leader will assume a network partition and will voluntarily yield its status, signalling a <code>LeaderFenced</code> event to the application.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.QueueTimeout</code></td>
      <td>30 s</td>
      <td>The maximum period of time a record may be queued after having been marked, before timing out and triggering a remark.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.MarkBackoff</code></td>
      <td>10 ms</td>
      <td>The backoff delay introduced by the mark thread when a query returns no results, indicating the absence of backlogged records. A mark backoff prevents aggressive querying of the database in the absence of a steady flow of outbox records.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.IOErrorBackoff</code></td>
      <td>500 ms</td>
      <td>The backoff delay introduced when any of the mark, purge or reset queries encounter a database error.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.MaxInFlightRecords</code></td>
      <td>1000</td>
      <td>An upper bound on the number of marked records that may be in flight at any given time. I.e. the number of records that have been enqueued with a producer client, for which acknowledgements have yet to be received.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.SendConcurrency</code></td>
      <td>8</td>
      <td>The number of concurrent shards used for queuing causally unrelated records. Each shard is equipped with a dedicated producer client, allowing for its records to be sent independently of other shards.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.SendBuffer</code></td>
      <td>10</td>
      <td>The maximum number of marked records that may be buffered for subsequent sending, for any given shard. When the buffer is full, the marker will halt — waiting for records to be sent and for their acknowledgements to flow through.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.MarkQueryRecords</code></td>
      <td>100</td>
      <td>An upper bound on the number of records that may be marked in any given query. Limiting this number avoids long-running database queries.</td>
    </tr>
    <tr valign="top">
      <td><code>Limits.MinMetricsInterval</code></td>
      <td>5 s</td>
      <td>The minimum interval at which throughput metrics are emitted. Metrics are emitted conservatively and may be observed less frequently; in fact, throughput metrics are only emitted upon a successful message acknowledgement, which will not occur during periods of inactivity.</td>
    </tr>
  </tbody>
</table>

# Docs
[Design](https://github.com/obsidiandynamics/goharvest/wiki/Design)

[Comparison of messaging patterns](https://github.com/obsidiandynamics/goharvest/wiki/Comparison-of-messaging-patterns)

[Comparison of harvesting methods](https://github.com/obsidiandynamics/goharvest/wiki/Comparison-of-harvesting-methods)

[FAQ](https://github.com/obsidiandynamics/goharvest/wiki/FAQ)


