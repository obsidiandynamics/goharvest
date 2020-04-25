package goharvest

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/obsidiandynamics/goharvest/metric"
)

// EventHandler is a callback function for handling GoHarvest events.
type EventHandler func(e Event)

// Event encapsulates a GoHarvest event.
type Event interface {
	fmt.Stringer
}

// LeaderAcquired is emitted upon successful acquisition of leader status.
type LeaderAcquired struct {
	leaderID uuid.UUID
}

// String obtains a textual representation of the LeaderAcquired event.
func (e LeaderAcquired) String() string {
	return fmt.Sprint("LeaderAcquired[leaderID=", e.leaderID, "]")
}

// LeaderID returns the local UUID of the elected leader.
func (e LeaderAcquired) LeaderID() uuid.UUID {
	return e.leaderID
}

// LeaderRefreshed is emitted when a new leader ID is generated as a result of a remarking request.
type LeaderRefreshed struct {
	leaderID uuid.UUID
}

// String obtains a textual representation of the LeaderRefreshed event.
func (e LeaderRefreshed) String() string {
	return fmt.Sprint("LeaderRefreshed[leaderID=", e.leaderID, "]")
}

// LeaderID returns the local UUID of the elected leader.
func (e LeaderRefreshed) LeaderID() uuid.UUID {
	return e.leaderID
}

// LeaderRevoked is emitted when the leader status has been revoked.
type LeaderRevoked struct{}

// String obtains a textual representation of the LeaderRevoked event.
func (e LeaderRevoked) String() string {
	return fmt.Sprint("LeaderRevoked[]")
}

// LeaderFenced is emitted when the leader status has been revoked.
type LeaderFenced struct{}

// String obtains a textual representation of the LeaderFenced event.
func (e LeaderFenced) String() string {
	return fmt.Sprint("LeaderFenced[]")
}

// MeterRead is emitted when the internal throughput Meter has been read.
type MeterRead struct {
	stats metric.MeterStats
}

// String obtains a textual representation of the MeterRead event.
func (e MeterRead) String() string {
	return fmt.Sprint("MeterRead[stats=", e.stats, "]")
}

// Stats embedded in the MeterRead event.
func (e MeterRead) Stats() metric.MeterStats {
	return e.stats
}
