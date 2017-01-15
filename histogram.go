// hdr package is a codahale/hdrhistogram.Histogram adaptor to
// rcrowley/go-metrics.Histogram interface.
package hdr

import (
	"sync"

	"github.com/codahale/hdrhistogram"
	metrics "github.com/rcrowley/go-metrics"
)

type histogram struct {
	hist  *hdrhistogram.Histogram
	mutex sync.Mutex
	reset bool
}

// New creates a new HDR histogram. If snapshotResets parameter is set to true
// histogram will be reset after each call to Snapshot(). This is useful only if
// single metrics reporter is running.
func New(snapshotResets bool, minValue, maxValue int64, sigfigs int) metrics.Histogram {
	h := &histogram{
		hist:  hdrhistogram.New(minValue, maxValue, sigfigs),
		reset: snapshotResets,
	}
	return h
}

func (h *histogram) Clear() {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.hist.Reset()
}

func (h *histogram) Count() int64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.hist.TotalCount()
}

func (h *histogram) Max() int64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.hist.Max()
}

func (h *histogram) Mean() float64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.hist.Mean()
}

func (h *histogram) Min() int64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.hist.Min()
}

func (h *histogram) Percentile(q float64) float64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return float64(h.hist.ValueAtQuantile(q))
}

func (h *histogram) Percentiles(qs []float64) []float64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	vals := make([]float64, len(qs))
	for i, q := range qs {
		vals[i] = float64(h.hist.ValueAtQuantile(q))
	}
	return vals
}

func (h *histogram) Sample() metrics.Sample {
	panic("Sample called on a Histogram")
}

func (h *histogram) Snapshot() metrics.Histogram {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	snap := &histogram{
		hist: hdrhistogram.Import(h.hist.Export()),
	}
	if h.reset {
		h.hist.Reset()
	}
	return snap
}

func (h *histogram) StdDev() float64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.hist.StdDev()
}

func (h *histogram) Sum() int64 {
	// HDR can not reproduce sum
	return 0
}

func (h *histogram) Update(v int64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.hist.RecordValue(v)
}

func (h *histogram) Variance() float64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	val := h.hist.StdDev()
	return val * val
}
