package testutil

import (
	"maps"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Series is one series a test expects: a metric name and its label values.
type Series struct {
	Name   string
	Labels map[string]string
}

// sample is what a test reads of one gathered series: the value of a
// counter or gauge, or the count and sum of a histogram.
type sample struct {
	labels map[string]string
	value  float64
	count  uint64
	sum    float64
	found  bool
}

// gather returns every series of g by metric name.
func gather(t testing.TB, g prometheus.Gatherer) map[string][]sample {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string][]sample{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			s := sample{labels: map[string]string{}, found: true}
			for _, l := range m.GetLabel() {
				s.labels[l.GetName()] = l.GetValue()
			}
			switch {
			case m.GetCounter() != nil:
				s.value = m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				s.value = m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				s.count = m.GetHistogram().GetSampleCount()
				s.sum = m.GetHistogram().GetSampleSum()
			}
			out[f.GetName()] = append(out[f.GetName()], s)
		}
	}
	return out
}

func find(all map[string][]sample, name string, labels map[string]string) sample {
	for _, s := range all[name] {
		if maps.Equal(s.labels, labels) {
			return s
		}
	}
	return sample{}
}

// MetricNames returns the sorted names of the metric families g serves.
func MetricNames(t testing.TB, g prometheus.Gatherer) []string {
	t.Helper()
	return slices.Sorted(maps.Keys(gather(t, g)))
}

// RequireSeries fails the test for every series of want that g does not
// serve.
func RequireSeries(t testing.TB, g prometheus.Gatherer, want ...Series) {
	t.Helper()
	all := gather(t, g)
	for _, w := range want {
		if labels := w.Labels; !find(all, w.Name, nonNil(labels)).found {
			t.Errorf("no series %s%v", w.Name, labels)
		}
	}
}

// SeriesCount returns the number of series of the metric name.
func SeriesCount(t testing.TB, g prometheus.Gatherer, name string) int {
	t.Helper()
	return len(gather(t, g)[name])
}

// MetricValue returns the value of the counter or gauge series name{labels};
// a series that does not exist fails the test.
func MetricValue(t testing.TB, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	s := find(gather(t, g), name, nonNil(labels))
	if !s.found {
		t.Fatalf("no series %s%v", name, labels)
	}
	return s.value
}

// HistogramSample returns the sample count and sum of the histogram series
// name{labels}; a series that does not exist fails the test.
func HistogramSample(t testing.TB, g prometheus.Gatherer, name string, labels map[string]string) (count uint64, sum float64) {
	t.Helper()
	s := find(gather(t, g), name, nonNil(labels))
	if !s.found {
		t.Fatalf("no series %s%v", name, labels)
	}
	return s.count, s.sum
}

// SeriesValues returns the value of every counter or gauge series of name,
// keyed by the value of its label key.
func SeriesValues(t testing.TB, g prometheus.Gatherer, name, key string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, s := range gather(t, g)[name] {
		out[s.labels[key]] += s.value
	}
	return out
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// Labels builds a label set from name, value pairs.
func Labels(kv ...string) map[string]string {
	out := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}
