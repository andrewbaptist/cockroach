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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type spyCollector struct {
	collectCount int
}

func (s *spyCollector) collect(disks []*monitoredDisk) error {
	s.collectCount++
	return nil
}

func TestMonitorManager_monitorDisks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manager := NewMonitorManager(vfs.NewMem())
	testDisk := &monitoredDisk{
		manager: manager,
		deviceID: DeviceID{
			major: 0,
			minor: 0,
		},
	}
	manager.mu.disks = []*monitoredDisk{testDisk}

	testCollector := &spyCollector{}
	stop := make(chan struct{})
	go manager.monitorDisks(testCollector, stop)

	time.Sleep(2 * defaultDiskStatsPollingInterval)
	stop <- struct{}{}
	require.Greater(t, testCollector.collectCount, 0)
}

func TestMonitor_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manager := NewMonitorManager(vfs.NewMem())
	testDisk := &monitoredDisk{
		manager: manager,
		deviceID: DeviceID{
			major: 0,
			minor: 0,
		},
		refCount: 2,
	}
	stop := make(chan struct{})
	manager.mu.stop = stop
	manager.mu.disks = []*monitoredDisk{testDisk}
	monitor1 := Monitor{monitoredDisk: testDisk}
	monitor2 := Monitor{monitoredDisk: testDisk}

	monitor1.Close()
	require.Equal(t, 1, testDisk.refCount)

	monitor1.Close()
	// Subsequent calls to a closed monitor should not reduce refCount.
	require.Equal(t, 1, testDisk.refCount)

	go monitor2.Close()
	// If there are no monitors, stop the stat polling loop.
	select {
	case <-stop:
	case <-time.After(time.Second):
		t.Fatal("Failed to receive stop signal")
	}
	require.Equal(t, 0, testDisk.refCount)
}

func TestMonitor_IncrementalStats(t *testing.T) {
	now := time.Now()
	testDisk := monitoredDisk{
		tracer: newMonitorTracer(3),
	}
	monitor := Monitor{monitoredDisk: &testDisk}

	testDisk.recordStats(now.Add(-10*time.Second), Stats{
		ReadsCount:      1,
		InProgressCount: 3,
	})
	// First attempt at getting incremental stats should fail since no data was
	// collected at the specified time.
	stats, err := monitor.IncrementalStats(time.Minute)
	require.Error(t, err)
	require.Equal(t, Stats{}, stats)

	testDisk.recordStats(now, Stats{
		ReadsCount:      2,
		InProgressCount: 2,
	})
	wantIncremental := Stats{
		ReadsCount: 1,
		// InProgressCount is a gauge so the increment should not be computed.
		InProgressCount: 2,
	}
	// Tracer should compute diff using data recorded over 10 seconds ago.
	stats, err = monitor.IncrementalStats(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, wantIncremental, stats)
}
