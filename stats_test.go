package main

import (
	"math"
	"testing"
)

func TestCalculateStatisticsEvenSample(t *testing.T) {
	samples := []float64{40.0, 10.0, 30.0, 20.0}
	stats := calculateStatistics(samples)

	if stats.Min != 10.0 || stats.Max != 40.0 {
		t.Fatalf("unexpected min/max: %#v", stats)
	}
	if stats.Mean != 25.0 {
		t.Fatalf("expected mean 25, got %v", stats.Mean)
	}
	if stats.Median != 25.0 {
		t.Fatalf("expected median 25, got %v", stats.Median)
	}
	if stats.Q1 != 10.0 {
		t.Fatalf("expected q1 10, got %v", stats.Q1)
	}
	if stats.Q3 != 30.0 {
		t.Fatalf("expected q3 30, got %v", stats.Q3)
	}
	if diff := math.Abs(stats.StdDev - 11.180339887); diff > 1e-9 {
		t.Fatalf("unexpected stddev: %v", stats.StdDev)
	}
}

func TestCalculateStatisticsOddSample(t *testing.T) {
	stats := calculateStatistics([]float64{3, 1, 2})
	if stats.Min != 1 || stats.Max != 3 {
		t.Fatalf("unexpected min/max: %#v", stats)
	}
	if stats.Mean != 2 {
		t.Fatalf("expected mean 2, got %v", stats.Mean)
	}
	if stats.Median != 2 {
		t.Fatalf("expected median 2, got %v", stats.Median)
	}
	if stats.Q1 != 1 || stats.Q3 != 3 {
		t.Fatalf("unexpected quartiles: Q1=%v Q3=%v", stats.Q1, stats.Q3)
	}
}

func TestCalculateStatisticsEmpty(t *testing.T) {
	stats := calculateStatistics(nil)
	if stats != (Stats{}) {
		t.Fatalf("expected zero-value stats, got %#v", stats)
	}
}
