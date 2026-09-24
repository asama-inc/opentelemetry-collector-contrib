// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	defaultComponentSyncWorkflow  = "ComponentSyncWorkflow"
	defaultComponentSyncTaskQueue = "ciss-inventory-tasks"
	defaultTemporalNamespace      = "default"
)

// ComponentSyncConfig starts ComponentSyncWorkflow on Temporal when a watched metric changes.
type ComponentSyncConfig struct {
	TemporalAddress string        `mapstructure:"temporal_address"`
	Namespace       string        `mapstructure:"namespace"`
	TaskQueue       string        `mapstructure:"task_queue"`
	Workflow        string        `mapstructure:"workflow"`
	Tenant          string        `mapstructure:"tenant"`
	Timeout         time.Duration `mapstructure:"timeout"`
}

func (c *ComponentSyncConfig) normalized() ComponentSyncConfig {
	out := *c
	if strings.TrimSpace(out.Namespace) == "" {
		out.Namespace = defaultTemporalNamespace
	}
	if strings.TrimSpace(out.TaskQueue) == "" {
		out.TaskQueue = defaultComponentSyncTaskQueue
	}
	if strings.TrimSpace(out.Workflow) == "" {
		out.Workflow = defaultComponentSyncWorkflow
	}
	out.Tenant = strings.TrimSpace(out.Tenant)
	out.TemporalAddress = strings.TrimSpace(out.TemporalAddress)
	return out
}

type componentSyncWorkflowInput struct {
	Tenant     string   `json:"tenant"`
	Hostname   string   `json:"hostname"`
	Components []string `json:"components"`
	RequestID  string   `json:"request_id,omitempty"`
}

type componentSyncStarter interface {
	StartComponentSync(ctx context.Context, hostname, metric, requestID string) error
	Close()
}

type temporalComponentSyncStarter struct {
	client client.Client
	cfg    ComponentSyncConfig
}

func newTemporalComponentSyncStarter(cfg ComponentSyncConfig) (*temporalComponentSyncStarter, error) {
	cfg = cfg.normalized()
	tc, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("temporal dial: %w", err)
	}
	return &temporalComponentSyncStarter{client: tc, cfg: cfg}, nil
}

func (s *temporalComponentSyncStarter) Close() {
	if s != nil && s.client != nil {
		s.client.Close()
	}
}

func (s *temporalComponentSyncStarter) StartComponentSync(ctx context.Context, hostname, metric, requestID string) error {
	hostname = strings.TrimSpace(hostname)
	metric = strings.TrimSpace(metric)
	if hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	components := componentsForMetric(metric)
	if len(components) == 0 {
		return fmt.Errorf("metric %q has no component sync mapping", metric)
	}

	input := componentSyncWorkflowInput{
		Tenant:     s.cfg.Tenant,
		Hostname:   hostname,
		Components: components,
		RequestID:  strings.TrimSpace(requestID),
	}
	workflowID := componentSyncWorkflowID(s.cfg.Tenant, hostname, components)
	_, err := s.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             s.cfg.TaskQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}, s.cfg.Workflow, input)
	if err != nil {
		return fmt.Errorf("temporal execute workflow: %w", err)
	}
	return nil
}

func componentSyncWorkflowID(tenant, hostname string, components []string) string {
	h := strings.TrimSpace(hostname)
	if h == "" {
		h = "unknown"
	}
	return h + "/component-sync/" + workflowIDComponent(componentKey(components)) + "/" + workflowIDComponent(tenant)
}

func componentKey(components []string) string {
	if len(components) == 0 {
		return "none"
	}
	out := make([]string, 0, len(components))
	for _, c := range components {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "-")
}

func workflowIDComponent(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return "unknown"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}
