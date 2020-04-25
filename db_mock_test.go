package goharvest

import (
	"github.com/google/uuid"
	"github.com/obsidiandynamics/libstdgo/concurrent"
)

type dbMockFuncs struct {
	Mark    func(m *dbMock, leaderID uuid.UUID, limit int) ([]OutboxRecord, error)
	Purge   func(m *dbMock, id int64) (bool, error)
	Reset   func(m *dbMock, id int64) (bool, error)
	Dispose func(m *dbMock)
}

type dbMockCounts struct {
	Mark,
	Purge,
	Reset,
	Dispose concurrent.AtomicCounter
}

type dbMock struct {
	markedRecords chan []OutboxRecord
	f             dbMockFuncs
	c             dbMockCounts
}

func (m *dbMock) Mark(leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
	defer m.c.Mark.Inc()
	return m.f.Mark(m, leaderID, limit)
}

func (m *dbMock) Purge(id int64) (bool, error) {
	defer m.c.Purge.Inc()
	return m.f.Purge(m, id)
}

func (m *dbMock) Reset(id int64) (bool, error) {
	defer m.c.Reset.Inc()
	return m.f.Reset(m, id)
}

func (m *dbMock) Dispose() {
	defer m.c.Dispose.Inc()
	m.f.Dispose(m)
}

func (m *dbMock) fillDefaults() {
	if m.markedRecords == nil {
		m.markedRecords = make(chan []OutboxRecord)
	}

	if m.f.Mark == nil {
		m.f.Mark = func(m *dbMock, leaderID uuid.UUID, limit int) ([]OutboxRecord, error) {
			select {
			case records := <-m.markedRecords:
				return records, nil
			default:
				return []OutboxRecord{}, nil
			}
		}
	}
	if m.f.Purge == nil {
		m.f.Purge = func(m *dbMock, id int64) (bool, error) {
			return true, nil
		}
	}
	if m.f.Reset == nil {
		m.f.Reset = func(m *dbMock, id int64) (bool, error) {
			return true, nil
		}
	}
	if m.f.Dispose == nil {
		m.f.Dispose = func(m *dbMock) {}
	}
	m.c.Mark = concurrent.NewAtomicCounter()
	m.c.Purge = concurrent.NewAtomicCounter()
	m.c.Reset = concurrent.NewAtomicCounter()
	m.c.Dispose = concurrent.NewAtomicCounter()
}

func mockDatabaseBindingProvider(m *dbMock) func(string, string) (DatabaseBinding, error) {
	return func(dataSource string, table string) (DatabaseBinding, error) {
		return m, nil
	}
}
