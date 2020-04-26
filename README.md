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

# How `goharvest` works
Harvesting of the outbox table sounds straightforward, but there are several notable challenges:

1. **Contention** — although multiple processes may write to a single outbox table (typically, multiple instances of some µ-service), only one process should be responsible for publishing records. Without exclusivity, records may be published twice or out-of-order by contending processes.
2. **Availability** — when one publisher fails, another should take over as soon as possible, with minimal downtime. The failure detection mechanism should be robust with respect to false positives; otherwise, the exclusivity constraint may be compromised.
3. **Causality** — multiple database write operations may be happening concurrently, and some of these operations may be ordered relative to each other. Publishing order should agree with the causal order of insertion. This is particularly challenging as transactions may be rolled back or complete out of order, leaving gaps in identifiers that may or may not eventually be filled.
4. **At-least-once delivery** — ensuring messages are delivered at least once in their lifetime.
5. **State** — the publisher needs to maintain the state of its progress so that it can resume publishing if the process is brought down, or transfer its state over to a new publisher if necessary.

`goharvest` can be embedded into an existing Go process, or bootstrapped as a standalone application using the `reaper` CLI. The latter is convenient where the application writing to the outbox is not implemented in Go, in which case `reaper` can be deployed as a sidecar. `goharvest` functions identically in both deployment modes; in fact, `reaper` just embeds the `goharvest` library, adding some config on top.

## Contention and availability
`goharvest` solves #1 and #2 by using a modified form of the [NELI protocol](https://github.com/obsidiandynamics/neli) called *Fast NELI*. Rather than embedding a separate consensus protocol such as PAXOS or Raft, NELI piggy-backs on Kafka's existing leader election mechanism — the one used for electing group and transaction coordinators within Kafka. NELI provides the necessary consensus without forcing the `goharvest` maintainers to deal with the intricacies of group management and atomic broadcast, and without requiring additional external dependencies.

When `goharvest` starts, it does not know whether it is a leader or a standby process. It uses a Kafka consumer client to subscribe to an existing Kafka topic specified by `Config.LeaderTopic`, bound by the consumer group specified by `Config.LeaderGroupID`. As part of the subscription, `goharvest` registers a **rebalance callback** — to be notified of partition reassignments as they occur.

> **Note**: The values `Config.LeaderTopic` and `Config.LeaderGroupID` should be chosen distinctly for each logical group of competing processes, using a name that clearly identifies the application or service. For example, `billing-api`.

No matter the chosen topic, it will always (by definition) have *at least* one partition — **partition zero**. It may carry other partitions too — indexes *1* through to *N-1*, where *N* is the topic width, but we don't care about them. Ultimately, Kafka will assign *at most one owner to any given partition* — picking one consumer from the encompassing consumer group. (We say 'at most' because all consumers might be offline.) For partition zero, one process will be assigned ownership; others will be kept in a holding pattern — waiting for the current assignee to depart or for Kafka to rebalance partition assignments.

Having subscribed to the topic, the client will repeatedly poll Kafka for new messages. Kafka uses polling as a way of verifying *consumer liveness*. (Under the hood, a Kafka client sends periodic heartbeats, which are tied to topic polling.) Should a consumer stop polling, heartbeats will stop flowing and Kafka's group coordinator will presume the client has died — reassigning partition ownership among the remaining clients. The client issues a poll at an interval specified by `Config.Limits.MinPollInterval`, defaulting to 100 ms.

The rebalance callback straightforwardly determines leadership through partition assignment, where the latter is managed by Kafka's group coordinator. *The use of the callback requires a stable network connection to the Kafka cluster*; if a network partition occurs, another client may be granted partition ownership — an event which is not synchronized with the outgoing leader. 

> **Note**: While Kafka's internal heartbeats are used to signal client presence to the broker, they are not suitable for the *safe* handling of network partitions from a client's perspective. During a network partition, the rebalance listener will be invoked at some point after the session times out on the client, by which time the partition may be reassigned on the broker.

In addition to observing partition assignment changes, the owner of partition zero periodically publishes a heartbeat message to `Config.LeaderTopic`. The client also consumes messages from that topic — effectively observing its own heartbeats, and thereby asserting that it is connected to the cluster *and* still owns the partition in question. If no heartbeat is received within the period specified by `Config.Limits.ReceiveDeadline` (5 seconds by default), the leader will take the worst-case assumption that the partition will be reassigned, and will voluntarily relinquish leader status. If connectivity is later resumed while the process is still the owner of the partition on the broker, it will again receive a heartbeat, allowing it to resume the leader role. If the partition has been subsequently reassigned, no heartbeat messages will be received upon reconnection and the client will be forced to rejoin the group — the act of which will invoke the rebalance callback, effectively resetting the client.

Once a `goharvest` client assumes leader status, it will generate a random UUID named `leaderID` and will track that UUID internally. Kafka's group coordinator may later choose to reassign the partition to another `goharvest` client. This would happen if the current leader times out, or if the number of contending `goharvest` instances changes. (For example, due to an autoscaling event.) Either way, a leadership change is identified via the rebalance callback or the prolonged absence of heartbeats. When it loses leader status, the client will clear the internal `leaderID`. Even if the leader status is later reverted to a previous leader, a new `leaderID` will be generated. (The old `leaderID` is never reused.) In other words, the `leaderID` is unique across all *views* of a group's membership state.

The diagram below illustrates the [NELI](https://github.com/obsidiandynamics/neli) leader election protocol, which is provided by the [goNELI](https://github.com/obsidiandynamics/goneli) library.

<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goharvest/images/figure-leader-election.png" width="100%" alt="NELI leader election"/>


## Causality, at-least-once delivery and process state
Problems #3, #4 and #5 are collectively solved with one algorithm, which has since been coined **mark-purge/reset** (MP/R). `goharvest` requires an outbox table with the following basic structure. (The real table has more columns; the simplified one below is used to illustrate the concept.).

|Column(s)     |Type            |Unique?       |Description|
|:-------------|:---------------|--------------|:----------|
|id            |serial          |yes           |A monotonically increasing, database-generated ID.
|payload       |doesn't matter  |doesn't matter|One or more columns that describe the contents of the record and how the corresponding message should be published to Kafka. 
|...           |...             |...           |...
|leader_id     |varchar         |no            |The unique identifier of the current leader process that marks a record as being in flight.           

In its most basic form, the algorithm operates concurrently across two Goroutines — a **mark thread** and a **purge/reset thread**. (Using the term *thread* for brevity.) Note, the `goharvest` implementation is significantly more involved, employing a three-stage pipeline with sharding and several concurrent producer clients to maximise throughput. The description here is of the minimal algorithm.

Once leadership has been acquired, the mark thread will begin repeatedly issuing **mark** queries in a loop, interleaved with periodic polling of Kafka to maintain liveness. Prior to marking, the loop clears a flag named `forceRemark`. (More on that in a moment.) 

A mark query performs two tasks in *one atomic operation*:

1. Identifies the earliest records in the outbox table that either have a `null` `leader_id`, or have a `leader_id` value that is different from the current `leaderID` of the harvester. This intermediate set is ordered by the `id` column, and limited in size — so as to avoid long-running queries.
2. Changes the `leader_id` attribute of the records identified in the previous step to the supplied `leaderID`, returning all affected records.

> **Note**: Both Oracle and Postgres allow the above to be accomplished efficiently using a single `UPDATE... SET... RETURNING` query. SQL Server supports a similar query, using the `OUTPUT` clause. Other databases will require a transaction to execute the above atomically.

The returned records are sorted by `id` before being processed. Although the query operates on an ordered set internally for the `UPDATE` clause, the records emitted by the `RETURNING` clause may be arbitrarily ordered.

Marking has the effect of *escrowing* the records, tracking the leader's progress through the record backlog. Once the query returns, the marker may enqueue the records onto Kafka. The latter is an asynchronous operation — the records will be sent sometime in the future and, later still, they will be acknowledged on the lead broker(s) and all in-sync replicas. In the meantime, the marking of records may continue (subject to the throttle and barrier constraints), potentially publishing more records as a result. For every record published, the mark thread increments an atomic counter named `inFlightRecords`, tracking the number of in-flight records. A throttle is applied to the marking process: at most `Config.Limits.MaxInFlightRecords` may be outstanding before marking is allowed to resume. This value is 1,000 records by default. (See the [FAQ](#q-why-throttle-the-total-number-of-in-flight-records) for an explanation). In addition to `inFlightRecords`, a scoreboard named `inFlightKeys[key]` is used to attribute individual counters for each record key, effectively acting as a synchronization barrier — such that at most one record may exist in flight for any given key. (Explained in the [FAQ](#q-why-throttle-in-flight-records-by-key).)

> **Note**: A scoreboard is a compactly represented map of atomic counters, where a counter takes up a map slot only if it is not equal to zero.

At some point, acknowledgements for previously published records will start arriving, processed by the purge/reset thread. (The combination of the producer client's outbound buffer and asynchronous delivery report handling enables the pipelining of the send and acknowledge operations.) For every acknowledged (meaning it has been durably persisted with Kafka) record, the delivery handler will execute a **purge** query. This involves deleting the record corresponding to the ID of the acknowledged message. Once a record is deleted, there is no more work to be done for it. The `inFlightRecords` counter is decremented, and the corresponding `inFlightKeys[key]` barrier is lowered — permitting the next record to be dispatched for the same key.

Owing to the unreliable nature of networks and I/O devices, there may be an error publishing the record. If an error is identified, we need a way of telling the marking process to re-queue it. This is done using a **reset** query, clearing the `leader_id` attribute for the record in question. Having reset the leader ID, we set the `forceRemark` flag, which was mentioned earlier — signalling to the mark thread that its recently marked records are no longer fit for publishing, in that an *older* record has since been reinstated. Afterwards, `inFlightRecords` is decremented and the `inFlightKeys[key]` barrier is lowered. On the other side, the mark thread will detect this condition upon entering the `inFlightKeys[key]` barrier, aborting the publishing process and *generating a new leader ID*. This has the effect of remarking any unacknowledged records, including the recently reset record.

The following diagram illustrates the MP/R algorithm. Each of the numbered steps is explained below.

<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goharvest/images/figure-mpr.png" width="100%" alt="MP/R algorithm"/>

### Key variables
|Variable                    |Type         |Description                     |Used by|
|:---------------------------|:------------|:-------------------------------|:------|
|<code>leaderID</code>       |<code>UUID</code>|The currently assigned leader ID|Marker thread|
|<code>inFlightRecords</code>|Atomic <code>int64</code> counter|Tracks the number of records sent, for which delivery notifications are still outstanding.|Both threads|
|<code>inFlightKeys</code>   |Atomic scoreboard of counters (or <code>bool</code> flags), keyed by a <code>string</code> record key|Tracks the number of in-flight records for any given record key. Each value can only be <code>0</code> or <code>1</code> (for integer-based implementations), or <code>false</code>/<code>true</code> for Boolean flags.|Both threads|
|<code>forceRemark</code>    |Atomic <code>bool</code>|An indication that one or more sent records experienced delivery failures and have been re-queued on the outbox.|Both threads, but only the mark thread may clear it.|
|<code>consumer</code>       |Kafka consumer instance|Interface to Kafka for the purpose of leader election.|Marker thread|
|<code>producer</code>       &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;|Kafka producer instance|Interface to Kafka for publishing outbox messages|Both threads. The mark thread will publish records; the purge/reset thread will handle delivery notifications.|

### On the mark thread...
<table>
  <thead>
    <tr>
      <th>Step</th>
      <th colspan="5">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>A1</td>
      <td colspan="5">Check leader status.</td>
    </tr>
    <tr>
      <td>Condition</td>
      <td colspan="5"><b>Client has leader status</b> (meaning it is responsible for harvesting):</td>
    </tr>
    <tr>
      <td>A2</td>
      <td></td>
      <td colspan="4">Clear the <code>forceRemark</code> flag, giving the purge/reset thread an opportunity to raise it again, should a delivery error occur.</td>
    </tr>
    <tr>
      <td>A3</td>
      <td></td>
      <td colspan="4">Invoke the <code>mark</code> query, marking records for the current <code>leaderID</code> and returning the set of modified records.</td>
    </tr>
    <tr>
      <td>Loop</td>
      <td></td>
      <td colspan="4"><b>For each record in the marked batch:</b></td>
    </tr>
    <tr>
      <td>A4</td>
      <td></td>
      <td></td>
      <td colspan="3">Drain the <code>inFlightRecords</code> counter up to the specified <code>Config.MaxInFlightRecords</code>.</td>
    </tr>
    <tr>
      <td>A5</td>
      <td></td>
      <td></td>
      <td colspan="3">Drain the value of <code>inFlightKeys[key]</code> (where <code>key</code> is the record key), waiting until it reaches zero. At zero, the barrier is effectively <i>lowered</i>, and the thread may proceed. No records for the given key should be in flight upon successful barrier entry.</td>
    </tr>
    <tr>
      <td>Condition</td>
      <td></td>
      <td></td>
      <td colspan="3"><b>The <code>forceRemark</code> flag has been not been set</b> (implying that there were no failed delivery reports):</td>
    </tr>
    <tr>
      <td>A6</td>
      <td></td>
      <td></td>
      <td></td>
      <td colspan="2">Increment the value of <code>inFlightRecords</code>.</td>
    </tr>
    <tr>
      <td>A7</td>
      <td></td>
      <td></td>
      <td></td>
      <td colspan="2">Increment the value of <code>inFlightKeys[key]</code>, effectively raising the barrier for the given record key.</td>
    </tr>
    <tr>
      <td>A8</td>
      <td></td>
      <td></td>
      <td></td>
      <td colspan="2">Publish the record to Kafka, via the producer client shared among the mark and purge/reset threads. Publishing is asynchronous; messages will be sent at some point in the future, after the send method returns.</td>
    </tr>
    <tr>
      <td>Condition</td>
      <td></td>
      <td></td>
      <td colspan="3"><b>The <code>forceRemark</code> flag has been set by the purge/reset thread, implying that at least one failed delivery report was received between now and the time of the last mark.</b></td>
    </tr>
    <tr>
      <td>A9</td>
      <td></td>
      <td></td>
      <td></td>
      <td colspan="2">Refresh the local <code>leaderID</code> by generating a new random UUID. Later, this will have the effect of remarking in-flight records.</td>
    </tr>
    <tr>
      <td>Until</td>
      <td></td>
      <td colspan="4"><b>The <code>forceRemark</code> flag has been set or there are no more records in the current batch.</b></td>
    </tr>
  </tbody>
</table>

### On the purge/reset thread...
<table>
  <thead>
    <tr>
      <th>Step</th>
      <th colspan="5">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Condition</td>
      <td colspan="5"><b>A message was delivered successfully:</b></td>
    </tr>
    <tr>
      <td>B1</td>
      <td></td>
      <td colspan="4">Invoke the <code>purge</code>, removing the outbox record corresponding to the ID of the delivered message.</td>
    </tr>
    <tr>
      <td>Condition</td>
      <td colspan="5"><b>Delivery failed for a message:</b></td>
    </tr>
    <tr>
      <td>B2</td>
      <td></td>
      <td colspan="4">Invoke the <code>query</code>, setting the <code>leader_id</code> attribute of the corresponding outbox record to <code>null</code>. This brings it up on the marker's scope.</td>
    </tr>
    <tr>
      <td>B3</td>
      <td></td>
      <td colspan="4">Set the <code>forceRemark</code> flag, communicating to the mark thread that at least one record from one of its previous mark cycles could not be delivered.</td>
    </tr>
    <tr>
      <td>B4</td>
      <td colspan="5">Decrement the count of <code>inFlightRecords</code>, releasing any active throttles.</td>
    </tr>
    <tr>
      <td>B5</td>
      <td colspan="5">Decrement <code>inFlightRecords[key]</code> for the key of the failed record, thereby lowering the barrier.</td>
    </tr>
    <tr>
      <td></td>
      <td colspan="5">Return from the delivery notification callback.</td>
    </tr>
  </tbody>
</table>


Causality is satisfied by observing the following premise: 

> Given two causally related transactions *T0* and *T1*, they will be processed on the same application and their execution must be serial. Assuming a transaction isolation level of *Read Committed* (or stricter), their side-effects will also appear in series from the perspective of an observer — the `goharvest` marker. In other words, it may observe nothing (if neither transaction has completed), just *T0* (if *T1* has not completed), or *T0* followed by *T1*, but never *T1* followed by *T0*. (Transactions here refer to any read-write operation, not just those executing within a demarcated transaction scope.)

Even though marking is always done from the head-end of the outbox table, it is not necessarily monotonic due to the sparsity of records. The marking of records will result in them being skipped on the next pass, but latent effects of transactions that were in flight during the last marking phase may materialise 'behind' the marked records. This is because not all records are causally related, and multiple concurrent processes and threads may be contending for writes to the outbox table. Database-generated sequential IDs don't help here: a transaction *T0* may begin before an unrelated *T1* and may also obtain a lower number from the sequence, but it may still complete after *T1*. In other words, *T1* may be observed in the absence of *T0* for some time, then *T0* might appear later, occupying a slot before *T1* in the table. This phenomenon is illustrated below.

<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/goharvest/images/figure-sequence-gaps.png" width="100%" alt="Sequence gaps"/>

Even though 'gaps' may be filled retrospectively, this only applies to unrelated record pairs. This is not a problem for MP/R because marking does not utilise an offset; it always starts from the top of the table. Any given marking pass will pick up causally-related records in their intended order, while unrelated records may be identified in arbitrary order. (Consult the [FAQ](#q-what-are-some-of-the-other-approaches) for more information on how MP/R compares to its alternatives.) To account for delivery failures and leader takeovers, the use of a per-key barrier ensures that at most one record is in an indeterminate state for any given key at any given time. Should that record be retried for whatever reason — on the same process or a different one — the worst-case outcome is a duplicated record, but not one that is out of order with respect to its successor or predecessor.

State management is simplified because the query always starts at the head-end of the outbox table and only skips those records that the current leader has processed. There is no need to track the offset of the harvester's progress through the outbox table. Records that may have been in-flight on a failed process are automatically salvaged because their `leader_id` value will not match that of the current leader. This makes MP/R **unimodal** — it takes the same approach irrespective of whether a record has *never* been processed or because it is *no longer* being processed. In other words, there are no `if` statements in the code to distinguish between routine operation and the salvaging of in-flight records from failed processes.


# FAQ



