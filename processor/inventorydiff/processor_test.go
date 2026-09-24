// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type captureSender struct {
	mu   sync.Mutex
	logs []plog.Logs
	err  error
}

func (c *captureSender) Send(_ context.Context, ld plog.Logs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := plog.NewLogs()
	ld.CopyTo(cp)
	c.logs = append(c.logs, cp)
	return c.err
}

func (c *captureSender) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.logs)
}

func testProcessor(t *testing.T, metrics []string, sender changelogSender) *inventoryDiffProcessor {
	t.Helper()
	cfg := &Config{
		Metrics: metrics,
		Changelog: ChangelogExport{
			Endpoint: "http://127.0.0.1:9/v1/logs",
		},
	}
	require.NoError(t, cfg.Validate())
	p := newProcessor(zap.NewNop(), cfg)
	p.sender = sender
	p.now = func() time.Time { return time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC) }
	require.NoError(t, p.start(context.Background(), nil))
	return p
}

func gaugeMetrics(hostname, name string, series []SeriesPoint) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("hostname", hostname)
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()
	for _, sp := range series {
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(sp.Value)
		if sp.Timestamp != "" {
			if ts, err := time.Parse(time.RFC3339Nano, sp.Timestamp); err == nil {
				dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
			}
		} else {
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 9, 10, 8, 0, 0, 0, time.UTC)))
		}
		for k, v := range sp.Labels {
			dp.Attributes().PutStr(k, v)
		}
	}
	return md
}

func TestSeedNoEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	md := gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	})
	out, err := p.processMetrics(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, 1, out.ResourceMetrics().Len())
	require.Equal(t, 0, cap.count())
}

func TestNoChangeNoEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	series := []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}
	md := gaugeMetrics("host-a", "node_md_member_state", series)
	_, err := p.processMetrics(context.Background(), md)
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, 0, cap.count())
}

func TestValueChangeEmitsEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_bonding_active"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_bonding_active", []SeriesPoint{
		{Labels: map[string]string{"master": "bond0"}, Value: 2},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_bonding_active", []SeriesPoint{
		{Labels: map[string]string{"master": "bond0"}, Value: 1},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, cap.count())
	attrs := lastAttrs(t, cap)
	require.Equal(t, "update", attrs[attrAction])
	require.Equal(t, "node_bonding_active", attrs[attrMetric])
	require.Contains(t, attrs[attrPrechangeData], `"value":2`)
	require.Contains(t, attrs[attrPostchangeData], `"value":1`)
	require.NotEmpty(t, attrs[attrRequestID])
}

func TestLabelChangeEmitsEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "faulty"}, Value: 1},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, cap.count())
	attrs := lastAttrs(t, cap)
	require.Equal(t, "update", attrs[attrAction])
	require.Contains(t, attrs[attrPrechangeData], "in_sync")
	require.Contains(t, attrs[attrPostchangeData], "faulty")
}

func TestSeriesAddedEmitsEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
		{Labels: map[string]string{"array": "md0", "device": "sdc", "state": "spare"}, Value: 1},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, cap.count())
	attrs := lastAttrs(t, cap)
	require.Equal(t, "update", attrs[attrAction])
	require.Contains(t, attrs[attrPostchangeData], "sdc")
}

func TestSeriesRemovedEmitsEvent(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
		{Labels: map[string]string{"array": "md0", "device": "sdb", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)
	require.Equal(t, 1, cap.count())
}

func TestMetricAbsentFromBatchSkipped(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)

	// Host still present, watched metric not in this batch (e.g. storage/health) → no event.
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("hostname", "host-a")
	other := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	other.SetName("unrelated_metric")
	other.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1)

	_, err = p.processMetrics(context.Background(), md)
	require.NoError(t, err)
	require.Equal(t, 0, cap.count())

	// Same series again later → still no event (cache unchanged).
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)
	require.Equal(t, 0, cap.count())
}

func TestMetricPresentEmptySeriesEmitsDelete(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"node_md_member_state"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda", "state": "in_sync"}, Value: 1},
	}))
	require.NoError(t, err)

	// Metric name present with zero datapoints → real empty snapshot → delete.
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_state", nil))
	require.NoError(t, err)
	require.Equal(t, 1, cap.count())
	attrs := lastAttrs(t, cap)
	require.Equal(t, "delete", attrs[attrAction])
	require.Contains(t, attrs[attrPostchangeData], `"series":[]`)
}

func TestUnlistedMetricIgnored(t *testing.T) {
	cap := &captureSender{}
	p := testProcessor(t, []string{"watched_metric"}, cap)
	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "other_metric", []SeriesPoint{
		{Labels: map[string]string{"x": "1"}, Value: 1},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "other_metric", []SeriesPoint{
		{Labels: map[string]string{"x": "1"}, Value: 2},
	}))
	require.NoError(t, err)
	require.Equal(t, 0, cap.count())
}

func TestChangelogFailureStillForwardsMetrics(t *testing.T) {
	cap := &captureSender{err: context.DeadlineExceeded}
	p := testProcessor(t, []string{"m"}, cap)
	md1 := gaugeMetrics("host-a", "m", []SeriesPoint{{Labels: map[string]string{"a": "1"}, Value: 1}})
	_, err := p.processMetrics(context.Background(), md1)
	require.NoError(t, err)
	md2 := gaugeMetrics("host-a", "m", []SeriesPoint{{Labels: map[string]string{"a": "1"}, Value: 2}})
	out, err := p.processMetrics(context.Background(), md2)
	require.NoError(t, err)
	require.Equal(t, 1, out.MetricCount())
	require.Equal(t, 1, cap.count())
}

func TestDeepCopyIsolatesCache(t *testing.T) {
	s := newStateStore()
	labels := map[string]string{"state": "in_sync"}
	snap := MetricSnapshot{
		ObservedAt: "t0",
		Series:     []SeriesPoint{{Labels: labels, Value: 1}},
	}
	s.Set("h", "m", snap)
	labels["state"] = "faulty"
	prev, ok := s.Get("h", "m")
	require.True(t, ok)
	require.Equal(t, "in_sync", prev.Series[0].Labels["state"])
}

func TestCompareSnapshotsIgnoresTimestamps(t *testing.T) {
	a := MetricSnapshot{Series: []SeriesPoint{{Labels: map[string]string{"k": "v"}, Value: 1, Timestamp: "t1"}}}
	b := MetricSnapshot{Series: []SeriesPoint{{Labels: map[string]string{"k": "v"}, Value: 1, Timestamp: "t2"}}}
	require.True(t, compareSnapshots(a, b))
}

func lastAttrs(t *testing.T, c *captureSender) map[string]string {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.logs)
	ld := c.logs[len(c.logs)-1]
	lr := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	out := map[string]string{}
	lr.Attributes().Range(func(k string, v pcommon.Value) bool {
		out[k] = v.Str()
		return true
	})
	return out
}

func TestConfigValidate(t *testing.T) {
	require.Error(t, (&Config{}).Validate())
	require.Error(t, (&Config{Metrics: []string{"m"}}).Validate())
	require.NoError(t, (&Config{
		Metrics:   []string{"m"},
		Changelog: ChangelogExport{Endpoint: "http://x"},
	}).Validate())
	require.Error(t, (&Config{
		Metrics:   []string{"m"},
		Changelog: ChangelogExport{Endpoint: "http://x"},
		ComponentSync: &ComponentSyncConfig{Tenant: "t"},
	}).Validate())
	require.NoError(t, (&Config{
		Metrics:   []string{"m"},
		Changelog: ChangelogExport{Endpoint: "http://x"},
		ComponentSync: &ComponentSyncConfig{
			TemporalAddress: "localhost:7233",
			Tenant:          "nxtgen",
		},
	}).Validate())
}

type captureComponentSyncStarter struct {
	mu    sync.Mutex
	calls []struct {
		hostname, metric, requestID string
	}
	err  error
	done chan struct{}
}

func (c *captureComponentSyncStarter) StartComponentSync(_ context.Context, hostname, metric, requestID string) error {
	c.mu.Lock()
	c.calls = append(c.calls, struct{ hostname, metric, requestID string }{hostname, metric, requestID})
	c.mu.Unlock()
	if c.done != nil {
		c.done <- struct{}{}
	}
	return c.err
}

func (c *captureComponentSyncStarter) Close() {}

func (c *captureComponentSyncStarter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func TestComponentSyncTriggerOnChange(t *testing.T) {
	cap := &captureSender{}
	sync := &captureComponentSyncStarter{done: make(chan struct{}, 1)}
	p := testProcessor(t, []string{"node_md_member_info"}, cap)
	p.componentSync = sync

	_, err := p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_info", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sda"}, Value: 1},
	}))
	require.NoError(t, err)
	_, err = p.processMetrics(context.Background(), gaugeMetrics("host-a", "node_md_member_info", []SeriesPoint{
		{Labels: map[string]string{"array": "md0", "device": "sdb"}, Value: 1},
	}))
	require.NoError(t, err)

	waitComponentSync(t, sync.done)
	require.Equal(t, 1, cap.count())
	require.Equal(t, 1, sync.count())
	require.Equal(t, "host-a", sync.calls[0].hostname)
	require.Equal(t, "node_md_member_info", sync.calls[0].metric)
	require.NotEmpty(t, sync.calls[0].requestID)
}

func TestComponentSyncFailureStillForwardsMetrics(t *testing.T) {
	cap := &captureSender{}
	sync := &captureComponentSyncStarter{err: io.EOF, done: make(chan struct{}, 1)}
	p := testProcessor(t, []string{"m"}, cap)
	p.componentSync = sync

	md1 := gaugeMetrics("host-a", "m", []SeriesPoint{{Labels: map[string]string{"a": "1"}, Value: 1}})
	_, err := p.processMetrics(context.Background(), md1)
	require.NoError(t, err)
	md2 := gaugeMetrics("host-a", "m", []SeriesPoint{{Labels: map[string]string{"a": "1"}, Value: 2}})
	out, err := p.processMetrics(context.Background(), md2)
	require.NoError(t, err)
	waitComponentSync(t, sync.done)
	require.Equal(t, 1, out.MetricCount())
	require.Equal(t, 1, cap.count())
	require.Equal(t, 1, sync.count())
}

func waitComponentSync(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for component sync trigger")
	}
}

func TestFactory(t *testing.T) {
	f := NewFactory()
	require.Equal(t, "inventorydiff", f.Type().String())
}
