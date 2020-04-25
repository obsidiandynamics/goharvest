package goharvest

import (
	"testing"

	"github.com/google/uuid"
	"github.com/obsidiandynamics/goharvest/metric"
	"github.com/stretchr/testify/assert"
)

func TestLeaderAcquired_string(t *testing.T) {
	leaderID, _ := uuid.NewRandom()
	assert.Contains(t, LeaderAcquired{leaderID}.String(), "LeaderAcquired[")
	assert.Contains(t, LeaderAcquired{leaderID}.String(), leaderID.String())
}

func TestLeaderAcquired_getter(t *testing.T) {
	leaderID, _ := uuid.NewRandom()
	e := LeaderAcquired{leaderID}
	assert.Equal(t, leaderID, e.LeaderID())
}

func TestLeaderRefreshed_string(t *testing.T) {
	leaderID, _ := uuid.NewRandom()
	assert.Contains(t, LeaderRefreshed{leaderID}.String(), "LeaderRefreshed[")
	assert.Contains(t, LeaderRefreshed{leaderID}.String(), leaderID.String())
}

func TestLeaderRefreshed_getter(t *testing.T) {
	leaderID, _ := uuid.NewRandom()
	e := LeaderRefreshed{leaderID}
	assert.Equal(t, leaderID, e.LeaderID())
}

func TestLeaderRevoked_string(t *testing.T) {
	assert.Equal(t, "LeaderRevoked[]", LeaderRevoked{}.String())
}

func TestLeaderFenced_string(t *testing.T) {
	assert.Equal(t, "LeaderFenced[]", LeaderFenced{}.String())
}

func TestMeterStats_string(t *testing.T) {
	stats := metric.MeterStats{}
	assert.Contains(t, MeterRead{stats}.String(), "MeterRead[")
	assert.Contains(t, MeterRead{stats}.String(), stats.String())
}
