// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	typeStr   = "inventorydiff"
	stability = component.StabilityLevelDevelopment
)

// NewFactory creates the inventorydiff metrics processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, stability),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	oCfg := cfg.(*Config)
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	p := newProcessor(set.Logger, oCfg)
	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		p.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
}
