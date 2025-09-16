package main

import (
    "math"
    "sort"
)

type Stats struct {
    Min    float64
    Max    float64
    Mean   float64
    Median float64
    Q1     float64
    Q3     float64
    StdDev float64
}

func calculateStatistics(samples []float64) Stats {
    if len(samples) == 0 {
        return Stats{}
    }
    s := append([]float64(nil), samples...)
    sort.Float64s(s)
    n := len(s)
    min := s[0]
    max := s[n-1]

    mean := 0.0
    for _, v := range s {
        mean += v
    }
    mean /= float64(n)

    var median float64
    if n%2 == 1 {
        median = s[n/2]
    } else {
        median = (s[n/2-1] + s[n/2]) / 2.0
    }

    idx := func(f float64) int {
        i := int(math.Ceil(f) - 1)
        if i < 0 {
            i = 0
        }
        if i >= n {
            i = n - 1
        }
        return i
    }
    q1 := s[idx(float64(n)/4.0)]
    q3 := s[idx(float64(3*n)/4.0)]

    var sumsq float64
    for _, v := range s {
        d := v - mean
        sumsq += d * d
    }
    variance := sumsq / float64(n)
    stddev := math.Sqrt(variance)

    return Stats{Min: min, Max: max, Mean: mean, Median: median, Q1: q1, Q3: q3, StdDev: stddev}
}

