"""AC9 — Scan reduction: partition/cluster-filtered queries scan fewer bytes.

Prerequisite: synthetic data loaded from fixtures/synth_ddl_perf*.yaml into
the 10 hot-path clustered tables (run `python -m lib.synth fixtures/synth_ddl_perf.yaml`
and `python -m lib.synth fixtures/synth_ddl_perf_ods.yaml` before this test).

For each hot-path table:
  - Run a cluster-filtered query and an unfiltered scan
  - Read total_bytes_processed from BQ job statistics
  - Assert filtered < unfiltered

For each require_partition_filter table:
  - Confirm an unfiltered query is REJECTED by BQ
  - Confirm a date_key-filtered query runs and scans fewer bytes

All figures from measured BQ job runs, never estimated or invented.
Pass: 10/10 hot-path objects show scan reduction, 3/3 partition-filter rejections.
"""
from __future__ import annotations

import pytest

# ── Hot-path table definitions ────────────────────────────────────────────
# Each: (dataset, table, cluster_col, filter_value)
# The filter_value targets a specific cluster-column value present in the
# synthetic data (agent_sk=1, queue_sk=1, program_sk=1, client_sk=1).

CLUSTERED_TABLES = [
    # DM tables (9 hot-path)
    ("dm", "fact_interaction", "agent_sk", "agent_sk = 1"),
    ("dm", "fact_agent_activity", "agent_sk", "agent_sk = 1"),
    ("dm", "fact_queue_interval", "queue_sk", "queue_sk = 1"),
    ("dm", "fact_csat_survey", "program_sk", "program_sk = 1"),
    ("dm", "fact_billing_line", "client_sk", "client_sk = 1"),
    ("dm", "fact_adherence_daily", "agent_sk", "agent_sk = 1"),
    ("dm", "fact_ticket", "program_sk", "program_sk = 1"),
    ("dm", "agg_agent_daily", "agent_sk", "agent_sk = 1"),
    ("dm", "agg_queue_hourly", "queue_sk", "queue_sk = 1"),
    # ODS table (1 hot-path)
    ("ods", "ods_interaction", "agent_id", "agent_id = 10001"),
]

# Tables with require_partition_filter = true (must reject unfiltered queries)
PARTITION_FILTER_TABLES = [
    ("dm", "fact_interaction", "date_key"),
    ("dm", "fact_agent_activity", "date_key"),
    ("dm", "fact_queue_interval", "date_key"),
]


def _query_bytes(bq_engine, sql: str) -> int:
    """Run query with cache off, return total_bytes_processed."""
    stats = bq_engine.query_stats(sql, use_cache=False)
    return stats["bytes_scanned"]


def _fq(bq_engine, dataset: str, table: str) -> str:
    return f"`{bq_engine.cfg.project}.{dataset}.{table}`"


def _has_data(bq_engine, dataset: str, table: str) -> bool:
    """Check if a table has any rows (precondition for scan comparison)."""
    fq = _fq(bq_engine, dataset, table)
    # For tables with require_partition_filter, we must include a filter
    if (dataset, table) in [(d, t) for d, t, _ in PARTITION_FILTER_TABLES]:
        rows = bq_engine.query(
            f"SELECT COUNT(*) AS n FROM {fq} WHERE date_key = 20260101"
        )
    else:
        rows = bq_engine.query(f"SELECT COUNT(*) AS n FROM {fq} LIMIT 1")
    return rows[0]["n"] > 0


@pytest.mark.live_bq
class TestScanReduction:
    """AC9: cluster-filtered queries scan fewer bytes than unfiltered scans."""

    @pytest.mark.parametrize(
        "dataset,table,cluster_col,filter_expr",
        CLUSTERED_TABLES,
        ids=[f"{d}.{t}" for d, t, _, _ in CLUSTERED_TABLES],
    )
    def test_cluster_filter_reduces_scan(
        self, bq_engine, dataset, table, cluster_col, filter_expr
    ):
        """Cluster-filtered < unfiltered for each hot-path table."""
        fq = _fq(bq_engine, dataset, table)

        # Skip if no data loaded (prerequisite: run synth seeder first)
        if not _has_data(bq_engine, dataset, table):
            pytest.skip(f"{dataset}.{table} has no data — run synth seeder first")

        # For tables with require_partition_filter, unfiltered scans are blocked.
        # Use a broad partition filter (all partitions) as the "unfiltered" baseline.
        needs_partition_filter = (dataset, table) in [
            (d, t) for d, t, _ in PARTITION_FILTER_TABLES
        ]

        if needs_partition_filter:
            # "Unfiltered" = partition filter only (no cluster filter)
            unfiltered_sql = (
                f"SELECT COUNT(*) FROM {fq} WHERE date_key BETWEEN 20200101 AND 20301231"
            )
            # Filtered = partition filter + cluster filter
            filtered_sql = (
                f"SELECT COUNT(*) FROM {fq} "
                f"WHERE date_key BETWEEN 20200101 AND 20301231 AND {filter_expr}"
            )
        else:
            unfiltered_sql = f"SELECT COUNT(*) FROM {fq}"
            filtered_sql = f"SELECT COUNT(*) FROM {fq} WHERE {filter_expr}"

        unfiltered_bytes = _query_bytes(bq_engine, unfiltered_sql)
        filtered_bytes = _query_bytes(bq_engine, filtered_sql)

        assert filtered_bytes < unfiltered_bytes, (
            f"{dataset}.{table}: cluster filter on {cluster_col} did NOT reduce scan: "
            f"filtered={filtered_bytes}, unfiltered={unfiltered_bytes}"
        )

    @pytest.mark.parametrize(
        "dataset,table,partition_col",
        PARTITION_FILTER_TABLES,
        ids=[f"{d}.{t}" for d, t, _ in PARTITION_FILTER_TABLES],
    )
    def test_partition_filter_required(self, bq_engine, dataset, table, partition_col):
        """Unfiltered query REJECTED by BQ when require_partition_filter=true."""
        fq = _fq(bq_engine, dataset, table)

        with pytest.raises(Exception, match="(?i)partition"):
            # This should be rejected because require_partition_filter = true
            bq_engine.query(f"SELECT COUNT(*) FROM {fq}")

    @pytest.mark.parametrize(
        "dataset,table,partition_col",
        PARTITION_FILTER_TABLES,
        ids=[f"{d}.{t}" for d, t, _ in PARTITION_FILTER_TABLES],
    )
    def test_partition_filter_prunes(self, bq_engine, dataset, table, partition_col):
        """A date_key-filtered query scans fewer bytes than full table size."""
        fq = _fq(bq_engine, dataset, table)

        if not _has_data(bq_engine, dataset, table):
            pytest.skip(f"{dataset}.{table} has no data — run synth seeder first")

        # Single-partition query should scan far less than all partitions
        single_partition_sql = (
            f"SELECT COUNT(*) FROM {fq} WHERE {partition_col} = 20260101"
        )
        all_partitions_sql = (
            f"SELECT COUNT(*) FROM {fq} "
            f"WHERE {partition_col} BETWEEN 20200101 AND 20301231"
        )

        single_bytes = _query_bytes(bq_engine, single_partition_sql)
        all_bytes = _query_bytes(bq_engine, all_partitions_sql)

        assert single_bytes < all_bytes, (
            f"{dataset}.{table}: partition pruning NOT effective: "
            f"single_partition={single_bytes}, all_partitions={all_bytes}"
        )

    def test_benchmarked_10_hotpath_objects(self):
        """Sanity: verify we have exactly 10 hot-path objects configured."""
        assert len(CLUSTERED_TABLES) == 10

    def test_configured_3_partition_filter_tables(self):
        """Sanity: verify we have exactly 3 require_partition_filter tables."""
        assert len(PARTITION_FILTER_TABLES) == 3
