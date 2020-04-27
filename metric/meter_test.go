package metric

import (
	"testing"
	"time"

	"github.com/obsidiandynamics/libstdgo/check"
	"github.com/obsidiandynamics/libstdgo/scribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func wait(t *testing.T) check.Timesert {
	return check.Wait(t, 10*time.Second)
}

func TestMeterString(t *testing.T) {
	m := NewMeter("test-name", time.Second)
	str := m.String()
	require.Contains(t, str, "Meter[")
	require.Contains(t, str, m.name)
}

func TestMeterMaybeStats(t *testing.T) {
	m := NewMeter("test-name", time.Millisecond)
	m.Add(1)
	wait(t).UntilAsserted(func(t check.Tester) {
		s := m.MaybeStats()
		if assert.NotNil(t, s) {
			assert.Equal(t, "test-name", s.Name)
			assert.Equal(t, int64(1), s.TotalCount)
			assert.Equal(t, int64(1), s.IntervalCount)
		}
	})

	m.Add(2)

	wait(t).UntilAsserted(func(t check.Tester) {
		s := m.MaybeStats()
		if assert.NotNil(t, s) {
			assert.Equal(t, "test-name", s.Name)
			assert.Equal(t, int64(3), s.TotalCount)
			assert.Equal(t, int64(2), s.IntervalCount)
		}
	})

	m.Add(1)
	m.Reset()

	wait(t).UntilAsserted(func(t check.Tester) {
		s := m.MaybeStats()
		if assert.NotNil(t, s) {
			assert.Equal(t, "test-name", s.Name)
			assert.Equal(t, int64(0), s.TotalCount)
			assert.Equal(t, int64(0), s.IntervalCount)
		}
	})
}

func TestMeterMaybeStatsCall(t *testing.T) {
	m := NewMeter("test-name", time.Millisecond)
	m.Add(1)
	wait(t).UntilAsserted(func(t check.Tester) {
		var statsPtr *MeterStats
		called := m.MaybeStatsCall(func(stats MeterStats) {
			statsPtr = &stats
		})
		if assert.True(t, called) {
			assert.NotNil(t, statsPtr)
			assert.Equal(t, "test-name", statsPtr.Name)
			assert.Equal(t, int64(1), statsPtr.TotalCount)
			assert.Equal(t, int64(1), statsPtr.IntervalCount)
		} else {
			assert.Nil(t, statsPtr)
		}
	})
}

func TestMeterMaybeStatsLog(t *testing.T) {
	m := NewMeter("test-name", time.Millisecond)
	m.Add(1)

	mockscribe := scribe.NewMock()
	scr := scribe.New(mockscribe.Factories())
	wait(t).UntilAsserted(func(t check.Tester) {
		called := m.MaybeStatsLog(scr.I())
		if assert.True(t, called) {
			mockscribe.Entries().
				Having(scribe.LogLevel(scribe.Info)).
				Having(scribe.MessageContaining("test-name")).
				Assert(t, scribe.Count(1))
		} else {
			mockscribe.Entries().
				Assert(t, scribe.Count(0))
		}
	})
}
