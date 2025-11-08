package main

import (
	"bufio"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const samplingInterval = 10 * time.Millisecond

var rssBytesFunc = rssBytes

func measurePeakResidentMemory(fn func() (Stats, float64)) (Stats, float64, float64) {
	baseline := rssBytesFunc()
	peak := baseline

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(samplingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if current := rssBytesFunc(); current > peak {
					peak = current
				}
			case <-stop:
				return
			}
		}
	}()

	stats, duration := fn()
	close(stop)
	wg.Wait()

	if peak == 0 {
		peak = baseline
	}
	return stats, duration, peak
}

func rssBytes() float64 {
	if runtime.GOOS == "linux" {
		if v := rssFromProcStatm(); v > 0 {
			return v
		}
		if v := rssFromProcStatus(); v > 0 {
			return v
		}
	}
	return rssFromPS()
}

func rssFromProcStatm() float64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return float64(pages * uint64(os.Getpagesize()))
}

func rssFromProcStatus() float64 {
	file, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				return 0
			}
			kb, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return 0
			}
			return float64(kb * 1024)
		}
	}
	return 0
}

func rssFromPS() float64 {
	pid := os.Getpid()
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	value := strings.TrimSpace(string(output))
	if value == "" {
		return 0
	}
	kb, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return float64(kb * 1024)
}
