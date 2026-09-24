// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package inventorydiff // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/inventorydiff"

import "strings"

// componentsForMetric maps a VictoriaMetrics metric name to component sync targets.
// Keep aligned with CISS internal/inventory/metriccomponent/map.go.
func componentsForMetric(metric string) []string {
	metric = strings.TrimSpace(metric)
	switch metric {
	// storage / MD + block + vendor RAID (+ related)
	case "node_md_member_info", "node_md_array_info", "node_md_array_size_bytes",
		"node_block_device_info", "node_disk_info",
		"hwraid_controller_info", "hwraid_vd_info", "hwraid_vd_size_bytes",
		"hwraid_pd_info", "hwraid_pd_size_bytes",
		"redfish_storage_controller_info", "redfish_storage_volume_info", "redfish_storage_drive_info",
		"smartctl_device_info":
		return []string{"storage"}

	// network
	case "node_network_interface_info", "node_ethtool_link_info", "node_pcie_adapter_info":
		return []string{"network"}

	// compute
	case "dmidecode_memory_info":
		return []string{"memory"}
	case "dmidecode_processor_info":
		return []string{"processor"}

	// nfs
	case "node_filesystem_mountpoint_info", "node_filesystem_size_bytes":
		return []string{"nfs"}

	default:
		return nil
	}
}
