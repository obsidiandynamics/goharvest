package metric

import (
	"fmt"
	"time"

	"github.com/obsidiandynamics/libstdgo/scribe"
)

// MeterStats is an immutable snapshot of meter statistics.
type MeterStats struct {
	Name             string
	Start            time.Time
	TotalCount       int64
	TotalRatePerS    float64
	IntervalCount    int64
	IntervalRatePerS float64
}

// String produces a textual representation of a MeterStats object.
func (s MeterStats) String() string {
	return fmt.Sprintf("Meter <%s>: %d since %v, rate: %.3f current, %.3f average\n",
		s.Name, s.TotalCount, s.Start.Format(timeFormat), s.IntervalRatePerS, s.TotalRatePerS)
}

// Meter is a simple structure for tracking the volume of events observed from two points in time:
// 	 1. When the Meter object was created (or when it was last reset)
//   2. From the last snapshot point.
//
// A meter can be updated by adding more observations. Statistics can be periodically extracted from the
// meter, reflecting the total observed volume as well as the volume in the most recent period.
//
// A meter is not thread-safe. In the absence of locking, it should only be accessed from a single
// goroutine.
type Meter struct {
	name              string
	printInterval     time.Duration
	start             time.Time
	totalCount        int64
	lastIntervalStart time.Time
	lastCount         int64
	lastStats         MeterStats
}

const timeFormat = "2006-01-02T15:04:05"

// String produces a textual representation of a Meter object.
func (m Meter) String() string {
	return fmt.Sprint("Meter[name=", m.name,
		", snapshotInterval=", m.printInterval,
		", start=", m.start.Format(timeFormat),
		", totalCount=", m.totalCount,
		", lastIntervalStart=", m.lastIntervalStart.Format(timeFormat),
		", lastCount=", m.lastCount,
		", lastStats=", m.lastStats, "]")
}

// NewMeter constructs a new meter object, with a given name and snapshot interval. The actual snapshotting
// of meter statistics is the responsibility of the goroutine that owns the meter.
func NewMeter(name string, snapshotInterval time.Duration) *Meter {
	m := Meter{}
	m.name = name
	m.printInterval = snapshotInterval
	m.Reset()
	return &m
}

// Reset the meter to its initial state — clearing all counters and resetting the clocks.
func (m *Meter) Reset() {
	m.start = time.Now()
	m.totalCount = 0
	m.lastIntervalStart = m.start
	m.lastCount = 0
}

// Add a value to the meter, contributing to the overall count and to the current interval.
func (m *Meter) Add(amount int64) {
	m.totalCount += amount
}

func (m *Meter) MaybeStats() *MeterStats {
	now := time.Now()
	elapsedInIntervalMs := now.Sub(m.lastIntervalStart).Milliseconds()
	if elapsedInIntervalMs > m.printInterval.Milliseconds() {
		intervalCount := m.totalCount - m.lastCount
		intervalRatePerS := float64(intervalCount) / float64(elapsedInIntervalMs) * 1000.0
		m.lastCount = m.totalCount
		m.lastIntervalStart = now

		elapsedTotalMs := now.Sub(m.start).Milliseconds()
		totalRatePerS := float64(m.totalCount) / float64(elapsedTotalMs) * 1000.0

		m.lastStats = MeterStats{
			Name:             m.name,
			Start:            m.start,
			TotalCount:       m.totalCount,
			TotalRatePerS:    totalRatePerS,
			IntervalCount:    intervalCount,
			IntervalRatePerS: intervalRatePerS,
		}
		return &m.lastStats
	}
	return nil
}

type MeterStatsCallback func(stats MeterStats)

func (m *Meter) MaybeStatsCall(cb MeterStatsCallback) {
	s := m.MaybeStats()
	if s != nil {
		cb(*s)
	}
}

func (m *Meter) MaybeStatsLog(logger scribe.Logger) {
	s := m.MaybeStats()
	if s != nil {
		logger("%v", *s)
	}
}
