// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import "sync"

// stateStore holds last known snapshot per host + metric.
// Example key: "host-a\x00node_md_member_state"
type stateStore struct {
	mu sync.Mutex
	m  map[string]MetricSnapshot
}

func newStateStore() *stateStore {
	return &stateStore{m: make(map[string]MetricSnapshot)}
}

func stateKey(hostname, metric string) string {
	return hostname + "\x00" + metric
}

func (s *stateStore) Get(hostname, metric string) (MetricSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[stateKey(hostname, metric)]
	return v, ok
}

// Set always deep-copies so the cache owns private Labels maps / Series slice.
func (s *stateStore) Set(hostname, metric string, snap MetricSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[stateKey(hostname, metric)] = cloneSnapshot(snap)
}
