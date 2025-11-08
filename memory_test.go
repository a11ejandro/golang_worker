package main

import (
	"sync"
	"testing"
	"time"
)

func TestMeasurePeakResidentMemoryTracksPeak(t *testing.T) {
	readings := []float64{100, 180, 120}
	var mu sync.Mutex

	rssBytesFunc = func() float64 {
		mu.Lock()
		defer mu.Unlock()
		if len(readings) == 0 {
			return 180
		}
		v := readings[0]
		readings = readings[1:]
		return v
	}
	t.Cleanup(func() { rssBytesFunc = rssBytes })

	stats, duration, peak := measurePeakResidentMemory(func() (Stats, float64) {
		time.Sleep(2 * samplingInterval)
		return Stats{}, 0.25
	})

	if duration != 0.25 {
		t.Fatalf("unexpected duration: %v", duration)
	}
	if peak != 180 {
		t.Fatalf("expected peak 180, got %v", peak)
	}
	if stats != (Stats{}) {
		t.Fatalf("expected empty stats, got %#v", stats)
	}
}

func TestMeasurePeakResidentMemoryHandlesZeroBaseline(t *testing.T) {
	rssBytesFunc = func() float64 { return 0 }
	t.Cleanup(func() { rssBytesFunc = rssBytes })

	_, _, peak := measurePeakResidentMemory(func() (Stats, float64) {
		return Stats{}, 0.0
	})

	if peak != 0 {
		t.Fatalf("expected peak 0, got %v", peak)
	}
}
