// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComponentsForMetric(t *testing.T) {
	require.Equal(t, []string{"storage"}, componentsForMetric("node_md_member_info"))
	require.Equal(t, []string{"storage"}, componentsForMetric("node_block_device_info"))
	require.Equal(t, []string{"storage"}, componentsForMetric("hwraid_vd_info"))
	require.Equal(t, []string{"network"}, componentsForMetric("node_network_interface_info"))
	require.Equal(t, []string{"network"}, componentsForMetric("node_ethtool_link_info"))
	require.Equal(t, []string{"network"}, componentsForMetric("node_pcie_adapter_info"))
	require.Equal(t, []string{"memory"}, componentsForMetric("dmidecode_memory_info"))
	require.Equal(t, []string{"processor"}, componentsForMetric("dmidecode_processor_info"))
	require.Equal(t, []string{"nfs"}, componentsForMetric("node_filesystem_mountpoint_info"))
	require.Nil(t, componentsForMetric("unknown_metric"))
}

func TestComponentSyncWorkflowID(t *testing.T) {
	got := componentSyncWorkflowID("nxtgen", "asama-test-02", []string{"storage", "nfs"})
	require.Equal(t, "asama-test-02/component-sync/nfs-storage/nxtgen", got)
}

func TestComponentSyncConfigNormalized(t *testing.T) {
	cfg := (&ComponentSyncConfig{
		TemporalAddress: "localhost:7233",
		Tenant:          "nxtgen",
	}).normalized()
	require.Equal(t, defaultTemporalNamespace, cfg.Namespace)
	require.Equal(t, defaultComponentSyncTaskQueue, cfg.TaskQueue)
	require.Equal(t, defaultComponentSyncWorkflow, cfg.Workflow)
}
