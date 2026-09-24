// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// SeriesPoint is one time series under a metric.
type SeriesPoint struct {
	Labels    map[string]string `json:"labels"`
	Value     float64           `json:"value"`
	Timestamp string            `json:"timestamp"` // datapoint time (RFC3339); not used in compareSnapshots
}

// MetricSnapshot is all series for one metric on one host at one observation.
type MetricSnapshot struct {
	ObservedAt string        `json:"observed_at"`
	Series     []SeriesPoint `json:"series"`
}

// KeySet maps canonical label keys to values (timestamps ignored).
func (s MetricSnapshot) KeySet() map[string]float64 {
	out := make(map[string]float64, len(s.Series))
	for _, sp := range s.Series {
		out[seriesKey(sp.Labels)] = sp.Value
	}
	return out
}

func seriesKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, ",")
}

// compareSnapshots returns true when label keys and values match (timestamps ignored).
func compareSnapshots(a, b MetricSnapshot) bool {
	ak, bk := a.KeySet(), b.KeySet()
	if len(ak) != len(bk) {
		return false
	}
	for k, v := range ak {
		if bv, ok := bk[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func snapshotToJSON(s MetricSnapshot) string {
	b, err := json.Marshal(s)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// cloneSnapshot returns a deep copy with private Labels maps.
func cloneSnapshot(s MetricSnapshot) MetricSnapshot {
	out := MetricSnapshot{ObservedAt: s.ObservedAt}
	out.Series = make([]SeriesPoint, len(s.Series))
	for i, sp := range s.Series {
		labels := make(map[string]string, len(sp.Labels))
		for k, v := range sp.Labels {
			labels[k] = v
		}
		out.Series[i] = SeriesPoint{
			Labels:    labels,
			Value:     sp.Value,
			Timestamp: sp.Timestamp,
		}
	}
	return out
}

func resourceHostname(attrs pcommon.Map) string {
	for _, key := range []string{"hostname", "host.name", "host_name"} {
		if v, ok := attrs.Get(key); ok && v.Type() == pcommon.ValueTypeStr && v.Str() != "" {
			return v.Str()
		}
	}
	return ""
}

func attrsToMap(attrs pcommon.Map) map[string]string {
	out := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			out[k] = v.Str()
		case pcommon.ValueTypeBool:
			out[k] = strconv.FormatBool(v.Bool())
		case pcommon.ValueTypeInt:
			out[k] = strconv.FormatInt(v.Int(), 10)
		case pcommon.ValueTypeDouble:
			out[k] = strconv.FormatFloat(v.Double(), 'g', -1, 64)
		default:
			out[k] = v.AsString()
		}
		return true
	})
	return out
}

func tsRFC3339(ts pcommon.Timestamp) string {
	if ts == 0 {
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}

// hostnamesInMetrics returns distinct hostnames present in the batch.
func hostnamesInMetrics(md pmetric.Metrics) []string {
	seen := make(map[string]struct{})
	var out []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		h := resourceHostname(rms.At(i).Resource().Attributes())
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	return out
}

// snapshotFromMetrics builds a snapshot for one hostname + metric name from the batch.
// present is false when the metric name does not appear for that host (caller should skip).
// present is true with empty Series when the metric exists but has no datapoints.
func snapshotFromMetrics(md pmetric.Metrics, hostname, metricName string, now time.Time) (MetricSnapshot, bool) {
	snap := MetricSnapshot{
		ObservedAt: now.UTC().Format(time.RFC3339Nano),
		Series:     []SeriesPoint{},
	}
	present := false
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		if resourceHostname(rm.Resource().Attributes()) != hostname {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() != metricName {
					continue
				}
				present = true
				appendMetricSeries(&snap, m)
			}
		}
	}
	return snap, present
}

func appendMetricSeries(snap *MetricSnapshot, m pmetric.Metric) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			snap.Series = append(snap.Series, SeriesPoint{
				Labels:    attrsToMap(dp.Attributes()),
				Value:     numberValue(dp),
				Timestamp: tsRFC3339(dp.Timestamp()),
			})
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			snap.Series = append(snap.Series, SeriesPoint{
				Labels:    attrsToMap(dp.Attributes()),
				Value:     numberValue(dp),
				Timestamp: tsRFC3339(dp.Timestamp()),
			})
		}
	}
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return 0
	}
}
