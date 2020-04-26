package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/obsidiandynamics/goharvest"
	"github.com/obsidiandynamics/goharvest/metric"
	"github.com/obsidiandynamics/goharvest/stasher"
)

const recordsPerTxn = 20

func main() {
	var keys, records, interval int
	var dataSource, outboxTable, kafkaTopic string
	var blank bool
	flag.IntVar(&keys, "keys", -1, "Number of unique keys")
	flag.IntVar(&records, "records", -1, "Number of records to generate")
	flag.IntVar(&interval, "interval", 0, "Write interval (in milliseconds")
	flag.StringVar(&dataSource, "ds", "host=localhost port=5432 user=postgres password= dbname=postgres sslmode=disable", "Data source")
	flag.StringVar(&outboxTable, "outbox", "outbox", "Outbox table name")
	flag.StringVar(&kafkaTopic, "topic", "pump", "Kafka output topic name")
	flag.BoolVar(&blank, "blank", false, "Generate blank records (nil value)")
	flag.Parse()

	errorFunc := func(field string) {
		flag.PrintDefaults()
		panic(fmt.Errorf("required '-%s' has not been set", field))
	}
	if keys == -1 {
		errorFunc("keys")
	}
	if records == -1 {
		errorFunc("records")
	}

	fmt.Printf("Starting stasher; keys: %d, records: %d, interval: %d ms\n", keys, records, interval)
	fmt.Printf("  Data source: %s\n", dataSource)
	fmt.Printf("  Outbox table name: %s\n", outboxTable)

	db, err := sql.Open("postgres", dataSource)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	st := stasher.New(outboxTable)

	meter := metric.NewMeter("pump", 5*time.Second)

	var tx *sql.Tx
	var pre stasher.PreStash
	for i := 0; i < records; i++ {
		if i%recordsPerTxn == 0 {
			finaliseTx(tx)

			tx, err = db.Begin()
			if err != nil {
				panic(err)
			}
			pre, err = st.Prepare(tx)
			if err != nil {
				panic(err)
			}
		}

		rand := rand.Uint64()
		var value *string
		if !blank {
			value = goharvest.String(fmt.Sprintf("value-%x", rand))
		}

		rec := goharvest.OutboxRecord{
			KafkaTopic: kafkaTopic,
			KafkaKey:   fmt.Sprintf("key-%x", rand%uint64(keys)),
			KafkaValue: value,
			KafkaHeaders: goharvest.KafkaHeaders{
				goharvest.KafkaHeader{Key: "Seq", Value: strconv.Itoa(i)},
			},
		}
		err := pre.Stash(rec)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(interval * int(time.Millisecond)))
		meter.Add(1)
		meter.MaybeStatsLog(log.Printf)
	}
	finaliseTx(tx)
}

func finaliseTx(tx *sql.Tx) {
	if tx != nil {
		err := tx.Commit()
		if err != nil {
			panic(err)
		}
	}
}
