// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package disk

import (
	"bytes"
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type traceEvent struct {
	time  time.Time
	stats Stats
	err   error
}

// String implements fmt.Stringer.
func (t traceEvent) String() string {
	if t.err != nil {
		return fmt.Sprintf("%s\t\t%s", t.time.Format(time.RFC3339Nano), t.err.Error())
	}
	s := t.stats
	statString := fmt.Sprintf("%s\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%s\t%d\t%s\t%s\t%d\t%d\t%d\t%s\t%d\t%s",
		s.DeviceName, s.ReadsCount, s.ReadsMerged, s.ReadsSectors, s.ReadsDuration,
		s.WritesCount, s.WritesMerged, s.WritesSectors, s.WritesDuration,
		s.InProgressCount, s.CumulativeDuration, s.WeightedIODuration,
		s.DiscardsCount, s.DiscardsMerged, s.DiscardsSectors, s.DiscardsDuration,
		s.FlushesCount, s.FlushesDuration)
	return fmt.Sprintf("%s\t%s\tnil", t.time.Format(time.RFC3339Nano), statString)
}

// monitorTracer manages a ring buffer containing a history of disk stats.
// The tracer is designed such that higher-level components can apply
// aggregation functions to compute statistics over rolling windows and output
// detailed traces when failures are detected.
type monitorTracer struct {
	capacity int

	mu struct {
		syncutil.Mutex
		trace []traceEvent
		start int
		end   int
		size  int
	}
}

func newMonitorTracer(capacity int) *monitorTracer {
	return &monitorTracer{
		capacity: capacity,
		mu: struct {
			syncutil.Mutex
			trace []traceEvent
			start int
			end   int
			size  int
		}{
			trace: make([]traceEvent, capacity),
			start: 0,
			end:   0,
			size:  0,
		},
	}
}

// RecordEvent appends a traceEvent to the internal ring buffer. The
// implementation assumes that the event time specified during consecutive calls
// are strictly increasing.
func (m *monitorTracer) RecordEvent(event traceEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.trace[m.mu.end] = event
	m.mu.end = (m.mu.end + 1) % m.capacity
	if m.mu.size == m.capacity {
		m.mu.start = (m.mu.start + 1) % m.capacity
	} else {
		m.mu.size++
	}
}

// Find retrieves the traceEvent that occurred before and closest to a specified
// time, t. If no events occurred before t, an error is thrown.
func (m *monitorTracer) Find(t time.Time) (traceEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	offset, err := m.floorSearchLocked(t)
	if err != nil {
		return traceEvent{}, err
	}
	eventIdx := (m.mu.start + offset) % m.capacity
	return m.mu.trace[eventIdx], nil
}

// Latest retrieves stats from the last traceEvent that was queued. If the trace
// is empty we throw an error.
func (m *monitorTracer) Latest() (traceEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.size == 0 {
		return traceEvent{}, errors.Errorf("trace is empty")
	}
	// Since m.mu.end points to the next index to write at, we add m.capacity to
	// prevent an arithmetic modulus error in case m.mu.end is zero.
	latestIdx := (m.mu.end - 1 + m.capacity) % m.capacity
	return m.mu.trace[latestIdx], nil
}

// String implements fmt.Stringer.
func (m *monitorTracer) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.size == 0 {
		return ""
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintln(w, "Time\t"+
		"Device Name\tReads Completed\tReads Merged\tSectors Read\tRead Duration\t"+
		"Writes Completed\tWrites Merged\tSectors Written\tWrite Duration\t"+
		"IO in Progress\tIO Duration\tWeighted IO Duration\t"+
		"Discards Completed\tDiscards Merged\tSectors Discarded\tDiscard Duration\t"+
		"Flushes Completed\tFlush Duration\tError")
	prevStats := m.mu.trace[m.mu.start].stats
	for i := 1; i < m.mu.size; i++ {
		event := m.mu.trace[(m.mu.start+i)%m.capacity]
		delta := event.stats.delta(&prevStats)
		if event.err == nil {
			prevStats = event.stats
		}
		deltaEvent := traceEvent{
			time:  event.time,
			stats: delta,
			err:   event.err,
		}
		fmt.Fprintln(w, deltaEvent)
	}
	_ = w.Flush()

	return buf.String()
}

// floorSearchLocked retrieves the offset from the trace's start for the traceEvent
// that occurred before or at a specified time, t. If no events occurred before
// t, an error is thrown. Note that it is the responsibility of the caller to
// acquire the tracer mutex and the returned offset may become invalid after the
// mutex is released.
func (m *monitorTracer) floorSearchLocked(t time.Time) (int, error) {
	if m.mu.size == 0 {
		return -1, errors.Errorf("trace is empty")
	}
	// Apply binary search to find the offset of the traceEvent that occurred at
	// or after time t.
	offset, found := sort.Find(m.mu.size, func(i int) int {
		idx := (m.mu.start + i) % m.capacity
		return t.Compare(m.mu.trace[idx].time)
	})
	if found {
		return offset, nil
	}
	if offset == 0 {
		return -1, errors.Errorf("no event found in trace before or at time %s", t)
	}
	// Decrement offset since it currently points to the first traceEvent that
	// occurred after time t.
	return offset - 1, nil
}
