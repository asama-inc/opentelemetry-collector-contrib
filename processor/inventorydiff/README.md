# inventorydiff processor

Watches configured metric names on the **collector VM** otel-collector metrics pipeline.
Keeps an in-memory last snapshot per `(hostname, metric)` (deep-copied on write).
When the snapshot changes, emits an OTLP log event to `changelog.endpoint`.
Metrics are always forwarded unchanged (fail-open if changelog export fails).

## Config

```yaml
processors:
  inventorydiff:
    metrics:
      # storage / MD
      - node_md_member_info
      - node_md_array_info
      - node_md_array_size_bytes
      # storage / block + vendor RAID
      - node_block_device_info
      - hwraid_vd_info
      - hwraid_pd_info
      # network
      - node_network_interface_info
      - node_ethtool_link_info
      - node_pcie_adapter_info
      # compute
      - dmidecode_memory_info
      - dmidecode_processor_info
    changelog:
      endpoint: https://YOUR_PLATFORM_HOST/api/otel
      headers:
        Authorization: "Basic ..."
    # Optional: start ComponentSyncWorkflow on Temporal (async, fail-open).
    component_sync:
      temporal_address: temporal:7233
      namespace: default
      task_queue: ciss-inventory-tasks
      workflow: ComponentSyncWorkflow
      tenant: nxtgen
      timeout: 10s

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [inventorydiff, batch]
      exporters: [prometheusremotewrite]
```

Rebuild the collector image/binary from `cmd/otel-collector/builder-config.yaml` so `inventorydiff` is compiled in. YAML alone is not enough.

## Change detection

Compares label sets + values only (timestamps ignored):

- series added / removed
- label change
- value change
- watched metric present with zero series → `action=delete`, empty post series

Watched metrics **absent** from a batch are skipped (partial domain×tier OTLP pushes must not look like deletes).

## Event

OTLP log with `service.name=asama-inventory-changelog` and attributes:

- `request_id`, `action`, `hostname`, `metric`
- `prechange_data`, `postchange_data` (JSON snapshots)
- `pre_observed_at`, `post_observed_at`

## Component sync (optional)

When `component_sync` is set, each changelog also starts `ComponentSyncWorkflow` on Temporal.
The collector acts as a Temporal **client**; the CISS worker on `task_queue` runs the workflow and activity.

Workflow input (JSON):

```json
{"tenant":"nxtgen","hostname":"host-a","components":["storage"],"request_id":"..."}
```

Metric → component mapping matches CISS:

| Metrics | Component |
|---------|-----------|
| `node_md_*`, `node_block_device_info`, `hwraid_*`, … | `storage` |
| `node_network_interface_info`, `node_ethtool_link_info`, `node_pcie_adapter_info` | `network` |
| `dmidecode_memory_info` | `memory` |
| `dmidecode_processor_info` | `processor` |
| `node_filesystem_*` | `nfs` |

Workflow ID: `{hostname}/component-sync/{components}/{tenant}` with terminate-if-running reuse.

Requires `ciss worker` polling the same task queue. Temporal start is async and fail-open.

## ClickHouse + platform routing

**1. Recreate table** (logs-shaped, like `otel_configfiles`):

```bash
export CLICKHOUSE_HOST=YOUR_CLICKHOUSE_HOST
./migrations/001_identity_change.sh
```

**2. Platform `otel-exporter.yaml`** — add filter, exporter, pipeline; exclude from `logs/general` (same pattern as configfiles). See README section below / deploy notes.

**3. Restart** `otel-exporter`, then query:

```sql
SELECT Timestamp, Hostname, Metric, Action, RequestId, PrechangeData, PostchangeData
FROM otel.identity_change
ORDER BY Timestamp DESC
LIMIT 20
```
