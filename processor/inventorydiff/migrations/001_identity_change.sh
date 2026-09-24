#!/usr/bin/env bash
# Create/replace otel.identity_change for the stock clickhouseexporter.
# Same pattern as otel.otel_configfiles:
#   - standard OTEL log columns (optional via DEFAULT '' / 0 — not product-required)
#   - product columns as DEFAULT from ResourceAttributes / LogAttributes
#
# WARNING: DROP + CREATE. Back up if the table has data you need.
#
# Usage:
#   export CLICKHOUSE_HOST=YOUR_CLICKHOUSE_HOST
#   ./migrations/001_identity_change.sh
#
# Defaults: port=18123 user=default password=(empty) database=otel cluster=default

set -euo pipefail

: "${CLICKHOUSE_HOST:?set CLICKHOUSE_HOST}"
CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-18123}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-otel}"
CLICKHOUSE_CLUSTER="${CLICKHOUSE_CLUSTER:-default}"

AUTH="${CLICKHOUSE_USER}"
if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
  AUTH="${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}"
fi

URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/?database=${CLICKHOUSE_DB}"

run_sql() {
  local sql="$1"
  curl -sS --fail-with-body --user "${AUTH}" "${URL}" --data-binary "${sql}"
  echo
}

echo "Dropping old identity_change (if any) on cluster=${CLICKHOUSE_CLUSTER}..."
run_sql "DROP TABLE IF EXISTS ${CLICKHOUSE_DB}.identity_change ON CLUSTER ${CLICKHOUSE_CLUSTER} SYNC"

echo "Creating identity_change (OTEL columns optional; product columns from maps)..."
run_sql "$(cat <<EOF
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.identity_change ON CLUSTER ${CLICKHOUSE_CLUSTER}
(
    -- Standard OTEL log columns: present so the exporter can INSERT.
    -- All have DEFAULT — none are product-required / must-fill by hand.
    Timestamp DateTime64(9) DEFAULT now64(9) CODEC(Delta(8), ZSTD(1)),
    TimestampTime DateTime DEFAULT toDateTime(Timestamp),
    TraceId String DEFAULT '' CODEC(ZSTD(1)),
    SpanId String DEFAULT '' CODEC(ZSTD(1)),
    TraceFlags UInt8 DEFAULT 0,
    SeverityText LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    SeverityNumber UInt8 DEFAULT 0,
    ServiceName LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    Body String DEFAULT '' CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    ScopeName String DEFAULT '' CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) DEFAULT map() CODEC(ZSTD(1)),
    EventName String DEFAULT '' CODEC(ZSTD(1)),

    -- Product columns: not required on INSERT; filled from maps after exporter write.
    Hostname LowCardinality(String) DEFAULT if(ResourceAttributes['hostname'] != '', ResourceAttributes['hostname'], ResourceAttributes['host.name']) CODEC(ZSTD(1)),
    Tenant LowCardinality(String) DEFAULT ResourceAttributes['tenant.id'] CODEC(ZSTD(1)),
    Metric LowCardinality(String) DEFAULT LogAttributes['metric'] CODEC(ZSTD(1)),
    Action LowCardinality(String) DEFAULT LogAttributes['action'] CODEC(ZSTD(1)),
    RequestId String DEFAULT LogAttributes['request_id'] CODEC(ZSTD(1)),
    PrechangeData String DEFAULT LogAttributes['prechange_data'] CODEC(ZSTD(1)),
    PostchangeData String DEFAULT LogAttributes['postchange_data'] CODEC(ZSTD(1)),
    PreObservedAt DateTime64(9) DEFAULT parseDateTime64BestEffortOrZero(LogAttributes['pre_observed_at']) CODEC(ZSTD(1)),
    PostObservedAt DateTime64(9) DEFAULT parseDateTime64BestEffortOrZero(LogAttributes['post_observed_at']) CODEC(ZSTD(1)),

    INDEX idx_request_id RequestId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_hostname Hostname TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_metric Metric TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/otel/identity_change_v2', '{replica}')
PARTITION BY toDate(TimestampTime)
PRIMARY KEY (ServiceName, TimestampTime)
ORDER BY (ServiceName, TimestampTime, Timestamp)
SETTINGS index_granularity = 8192
EOF
)"

echo "OK. Describe:"
run_sql "DESCRIBE TABLE identity_change FORMAT PrettyCompact"
