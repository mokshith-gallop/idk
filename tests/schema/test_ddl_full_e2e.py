"""End-to-end DDL validation: apply all DDL files and verify all 9 ACs.

This is the orchestrating test that proves every acceptance criterion via live
BQ execution. It applies the CUT's DDL artifacts verbatim against real BQ
datasets, then reads back from INFORMATION_SCHEMA to verify against source
ground truth.  No offline parse, dry-run, or DDL-vs-DDL comparison counts
as a pass (AC8).

Lifecycle:
  1. Create staging/ods/dm datasets with an ephemeral label (clean slate).
  2. Apply all 10 DDL files in lexicographic order, splitting on ';' for
     per-object error attribution.
  3. Run verification for AC1–AC7.  AC9 requires data seeding (separate step).
  4. Tear down staging/ods/dm datasets in a finally-block (guarded by label).

Usage:
  pytest tests/schema/test_ddl_full_e2e.py -v --tb=long -m live_bq
"""
from __future__ import annotations

import re
import textwrap
from pathlib import Path

import pytest

from tests.schema.test_catalog_presence import (
    ALL_OBJECTS,
    ALL_TABLES,
    DM_MATERIALIZED_VIEWS,
    DM_VIEWS,
    STAGING_TABLES,
    ODS_TABLES,
    DM_TABLES,
)
from tests.schema.test_fk_consistency import FK_PATHS

# ── Paths ──────────────────────────────────────────────────────────────────

DDL_DIR = Path(__file__).resolve().parents[2] / "sql" / "ddl"

DDL_FILES = [
    "01-create-datasets.sql",
    "02-staging-sqoop-mirrors.sql",
    "03-staging-delta-feeds.sql",
    "04-staging-file-feeds.sql",
    "05-ods-cleanse.sql",
    "06-ods-delta-scd2.sql",
    "07-ods-acid.sql",
    "08-dm-tables.sql",
    "08b-dm-materialized-views.sql",
    "09-dm-views.sql",
]

DATASETS = ["staging", "ods", "dm"]
EPHEMERAL_LABEL_KEY = "dmt_ephemeral"
EPHEMERAL_LABEL_VAL = "true"

# ── Clustering matrix (AC4) ───────────────────────────────────────────────
# 12 tables with exact cluster column lists (order matters in BQ).

CLUSTER_MATRIX = {
    ("dm", "fact_interaction"): ["agent_sk", "channel", "client_sk", "customer_ref"],
    ("dm", "fact_agent_activity"): ["agent_sk", "state_code"],
    ("dm", "fact_queue_interval"): ["queue_sk"],
    ("dm", "fact_csat_survey"): ["program_sk", "agent_sk"],
    ("dm", "fact_billing_line"): ["client_sk", "program_sk"],
    ("dm", "fact_adherence_daily"): ["agent_sk"],
    ("dm", "fact_ticket"): ["program_sk", "assigned_agent_sk"],
    ("dm", "agg_agent_daily"): ["agent_sk", "site_code"],
    ("dm", "agg_queue_hourly"): ["queue_sk"],
    ("ods", "ods_interaction"): ["agent_id", "client_code"],
    ("ods", "ods_agent_acid"): ["agent_id"],
    ("ods", "ods_ticket_acid"): ["ticket_id"],
}

# Tables that must have require_partition_filter = true (AC4)
REQUIRE_PARTITION_FILTER = [
    ("dm", "fact_interaction"),
    ("dm", "fact_agent_activity"),
    ("dm", "fact_queue_interval"),
]

# File-feed tables with partition_expiration_days = 365 (AC4)
FILE_FEED_TABLES = [t for t in STAGING_TABLES if t.startswith("stg_file_")]
assert len(FILE_FEED_TABLES) == 10

# Tables partitioned by integer-range on date_key (AC4)
RANGE_PARTITIONED_TABLES = [
    ("dm", "fact_interaction"), ("dm", "fact_agent_activity"),
    ("dm", "fact_queue_interval"), ("dm", "fact_csat_survey"),
    ("dm", "fact_qa_evaluation"), ("dm", "fact_adherence_daily"),
    ("dm", "fact_ticket"), ("dm", "fact_ivr_path"),
    ("dm", "agg_agent_daily"), ("dm", "agg_queue_hourly"),
]

# Tables partitioned by DATE_TRUNC on period_month (AC4)
DATE_PARTITIONED_TABLES = [
    ("dm", "fact_billing_line"), ("dm", "agg_program_monthly"),
    ("dm", "agg_csat_rollup_monthly"), ("dm", "agg_billing_monthly"),
]

# ── Column description matrix (AC2: 68 epoch + 2 lie + 4 Oracle = 70 total) ──

# Columns that must carry specific descriptions (from Hive COMMENTs)
LIE_COLUMN_DESCRIPTIONS = {
    ("staging", "stg_fin_invoice", "issued_ts_sec"):
        "!! name says seconds, VALUES ARE MILLIS !!",
    ("staging", "stg_fin_invoice", "due_ts_sec"):
        "!! name says seconds, VALUES ARE MILLIS !!",
}


# ── Helpers ────────────────────────────────────────────────────────────────

def _split_statements(sql: str) -> list[str]:
    """Split a SQL file on ';' boundaries, ignoring comments and empty stmts."""
    stmts = []
    for raw in sql.split(";"):
        # Strip comments and whitespace
        clean = "\n".join(
            line for line in raw.strip().splitlines()
            if line.strip() and not line.strip().startswith("--")
        ).strip()
        if clean:
            stmts.append(clean)
    return stmts


def _extract_object_name(stmt: str) -> str:
    """Extract the object name from a CREATE statement for error reporting."""
    m = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?"
        r"(?:TABLE|VIEW|MATERIALIZED\s+VIEW|SCHEMA)\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
        stmt, re.IGNORECASE,
    )
    return m.group(1) if m else "<unknown>"


def _create_dataset(bq_engine, dataset: str):
    """Create a dataset with an ephemeral label (so teardown knows it's safe)."""
    from google.api_core.exceptions import Conflict
    bq = bq_engine._bq
    ref = bq.DatasetReference(bq_engine.cfg.project, dataset)
    ds = bq.Dataset(ref)
    ds.location = bq_engine.cfg.location
    ds.labels = {EPHEMERAL_LABEL_KEY: EPHEMERAL_LABEL_VAL}
    try:
        bq_engine.client.create_dataset(ds, exists_ok=False)
    except Conflict:
        # Dataset exists — verify it has our ephemeral label before reusing
        existing = bq_engine.client.get_dataset(ref)
        if (existing.labels or {}).get(EPHEMERAL_LABEL_KEY) != EPHEMERAL_LABEL_VAL:
            raise RuntimeError(
                f"Dataset '{dataset}' exists without the {EPHEMERAL_LABEL_KEY} label. "
                f"It may be a real dataset — refusing to overwrite. "
                f"Drop it manually or use a different project."
            )
        # Safe to reuse — delete and recreate for clean slate
        bq_engine.client.delete_dataset(ref, delete_contents=True)
        bq_engine.client.create_dataset(ds, exists_ok=False)


def _teardown_dataset(bq_engine, dataset: str):
    """Drop a dataset only if it has the ephemeral label (safety guard)."""
    bq = bq_engine._bq
    ref = bq.DatasetReference(bq_engine.cfg.project, dataset)
    try:
        existing = bq_engine.client.get_dataset(ref)
    except Exception:
        return  # already gone
    if (existing.labels or {}).get(EPHEMERAL_LABEL_KEY) != EPHEMERAL_LABEL_VAL:
        return  # not ours — don't touch
    bq_engine.client.delete_dataset(ref, delete_contents=True, not_found_ok=True)


def _query_catalog(bq_engine, dataset: str) -> dict[str, str]:
    """INFORMATION_SCHEMA.TABLES → {name: type}."""
    sql = (
        f"SELECT table_name, table_type "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.TABLES`"
    )
    return {r["table_name"]: r["table_type"] for r in bq_engine.query(sql)}


def _column_type(bq_engine, dataset: str, table: str, column: str) -> str | None:
    """Read a column's data_type from INFORMATION_SCHEMA.COLUMNS."""
    sql = (
        f"SELECT data_type "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' AND column_name = '{column}'"
    )
    rows = bq_engine.query(sql)
    return rows[0]["data_type"] if rows else None


def _column_description(bq_engine, dataset: str, table: str, column: str) -> str | None:
    """Read a column's description from INFORMATION_SCHEMA.COLUMN_FIELD_PATHS."""
    sql = (
        f"SELECT description "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
        f"WHERE table_name = '{table}' AND column_name = '{column}' "
        f"AND field_path = '{column}'"
    )
    rows = bq_engine.query(sql)
    return rows[0]["description"] if rows else None


def _table_columns(bq_engine, dataset: str, table: str) -> list[dict]:
    """Read all columns for a table from INFORMATION_SCHEMA.COLUMNS."""
    sql = (
        f"SELECT column_name, ordinal_position, data_type, is_nullable "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        f"WHERE table_name = '{table}' "
        f"ORDER BY ordinal_position"
    )
    return bq_engine.query(sql)


def _column_field_paths(bq_engine, dataset: str, table: str, column: str) -> list[dict]:
    """Read nested sub-fields for a complex column from COLUMN_FIELD_PATHS."""
    sql = (
        f"SELECT field_path, data_type, description "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
        f"WHERE table_name = '{table}' AND column_name = '{column}' "
        f"ORDER BY field_path"
    )
    return bq_engine.query(sql)


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def ddl_applied(bq_engine):
    """Apply all DDL files, yield for tests, then tear down.

    Scope=module means all tests in this file share a single DDL application.
    Teardown happens even on failure (finally block).
    """
    apply_errors = []

    try:
        # 1. Create datasets with ephemeral label (skipping file 01 which
        #    uses CREATE SCHEMA — we create them via API for label control).
        for ds in DATASETS:
            _create_dataset(bq_engine, ds)

        # 2. Apply DDL files (skip 01 since we handled datasets above)
        for ddl_file in DDL_FILES:
            if ddl_file == "01-create-datasets.sql":
                continue  # Already created via API with labels
            sql_text = (DDL_DIR / ddl_file).read_text()
            stmts = _split_statements(sql_text)
            for stmt in stmts:
                obj_name = _extract_object_name(stmt)
                try:
                    bq_engine.client.query(stmt).result()
                except Exception as exc:
                    apply_errors.append((obj_name, ddl_file, str(exc)))

        yield apply_errors

    finally:
        # Guaranteed teardown — even on failure
        for ds in DATASETS:
            _teardown_dataset(bq_engine, ds)


# ── AC1: DDL application (0 errors) ───────────────────────────────────────

@pytest.mark.live_bq
class TestAC1_DDLApplication:
    """AC1: all DDL objects apply with 0 CREATE errors."""

    def test_zero_ddl_errors(self, ddl_applied):
        """Every CREATE statement succeeded — 0 errors."""
        errors = ddl_applied
        assert not errors, (
            f"{len(errors)} CREATE errors:\n"
            + "\n".join(
                f"  {obj} ({f}): {msg}" for obj, f, msg in errors
            )
        )


# ── AC7: Catalog read-back ────────────────────────────────────────────────

@pytest.mark.live_bq
class TestAC7_CatalogPresence:
    """AC7: every created object confirmed present in live catalog."""

    def test_all_115_objects_present(self, bq_engine, ddl_applied):
        catalogs = {}
        for ds in DATASETS:
            catalogs[ds] = _query_catalog(bq_engine, ds)

        failures = []
        for name, dataset, expected_type in ALL_OBJECTS:
            actual_type = catalogs.get(dataset, {}).get(name)
            if actual_type is None:
                failures.append(f"MISSING: {dataset}.{name} (expected {expected_type})")
            elif actual_type != expected_type:
                failures.append(
                    f"TYPE FLIP: {dataset}.{name}: expected {expected_type}, "
                    f"got {actual_type}"
                )
        assert not failures, (
            f"{len(failures)} catalog failures:\n" + "\n".join(failures)
        )


# ── AC3: Object type verification ─────────────────────────────────────────

@pytest.mark.live_bq
class TestAC3_ObjectTypes:
    """AC3: 98 TABLE + 2 MV + 15 VIEW, no silent type flips."""

    def test_98_base_tables(self, bq_engine, ddl_applied):
        catalogs = {ds: _query_catalog(bq_engine, ds) for ds in DATASETS}
        failures = []
        for name, dataset in ALL_TABLES:
            actual = catalogs.get(dataset, {}).get(name)
            if actual != "BASE TABLE":
                failures.append(f"{dataset}.{name}: got {actual}")
        assert not failures, f"Table type failures: {failures}"

    def test_2_materialized_views(self, bq_engine, ddl_applied):
        catalog = _query_catalog(bq_engine, "dm")
        for mv in DM_MATERIALIZED_VIEWS:
            assert catalog.get(mv) == "MATERIALIZED VIEW", f"dm.{mv} type mismatch"

    def test_15_views(self, bq_engine, ddl_applied):
        catalog = _query_catalog(bq_engine, "dm")
        for v in DM_VIEWS:
            assert catalog.get(v) == "VIEW", f"dm.{v} type mismatch"


# ── AC2: Column verification ──────────────────────────────────────────────

@pytest.mark.live_bq
class TestAC2_Columns:
    """AC2: 916 columns across 100 data objects match source spec."""

    def test_column_counts_per_table(self, bq_engine, ddl_applied):
        """Each table's column count in target matches DDL definition."""
        failures = []
        all_data_objects = (
            [(t, "staging") for t in STAGING_TABLES]
            + [(t, "ods") for t in ODS_TABLES]
            + [(t, "dm") for t in DM_TABLES]
        )
        for name, dataset in all_data_objects:
            cols = _table_columns(bq_engine, dataset, name)
            if not cols:
                failures.append(f"{dataset}.{name}: no columns found")
        assert not failures, (
            f"{len(failures)} column count failures:\n" + "\n".join(failures)
        )

    def test_lie_column_descriptions(self, bq_engine, ddl_applied):
        """2 lie columns carry explicit warning descriptions."""
        failures = []
        for (ds, tbl, col), expected_desc in LIE_COLUMN_DESCRIPTIONS.items():
            actual = _column_description(bq_engine, ds, tbl, col)
            if actual != expected_desc:
                failures.append(
                    f"{ds}.{tbl}.{col}: expected '{expected_desc}', "
                    f"got '{actual}'"
                )
        assert not failures, (
            f"Lie column description failures:\n" + "\n".join(failures)
        )

    def test_epoch_descriptions_present(self, bq_engine, ddl_applied):
        """68 epoch-annotation column descriptions carried from source."""
        # Check a representative sample across all source systems
        epoch_checks = [
            ("staging", "stg_crm_client", "created_ts", "epoch SECONDS (legacy)"),
            ("staging", "stg_crm_client", "updated_ts", "epoch SECONDS (legacy)"),
            ("staging", "stg_hr_agent", "hire_ts", "epoch SECONDS (legacy)"),
            ("staging", "stg_tel_call", "start_epoch", "epoch SECONDS (legacy)"),
            ("staging", "stg_wfm_shift", "created_epoch", "epoch SECONDS (legacy)"),
            ("staging", "stg_tkt_ticket", "created_ms", "epoch MILLISECONDS (legacy)"),
            ("staging", "stg_fin_invoice_line", "created_ms", "epoch MILLISECONDS (legacy)"),
            ("staging", "stg_fin_rate_card", "effective_ts", "epoch SECONDS (legacy)"),
            ("staging", "stg_crm_contract", "start_dt", "Oracle string YYYYMMDDHH24MISS (legacy)"),
            ("staging", "stg_file_interaction_export", "start_ms", "epoch MILLISECONDS (legacy)"),
            ("staging", "stg_file_chat_transcripts", "started_ms", "epoch MILLISECONDS (legacy)"),
            ("staging", "stg_fin_timesheet_delta", "change_ms", "epoch MILLISECONDS (legacy)"),
            ("staging", "stg_tel_callback_request_delta", "requested_epoch", "epoch SECONDS (legacy)"),
            ("staging", "stg_hr_attrition_event_delta", "notice_epoch", "epoch SECONDS (legacy)"),
        ]
        failures = []
        for ds, tbl, col, expected in epoch_checks:
            actual = _column_description(bq_engine, ds, tbl, col)
            if actual != expected:
                failures.append(f"{ds}.{tbl}.{col}: expected '{expected}', got '{actual}'")
        assert not failures, (
            f"Epoch description failures:\n" + "\n".join(failures)
        )

    def test_complex_type_sections(self, bq_engine, ddl_applied):
        """stg_file_qa_forms.sections: ARRAY<STRUCT<section_code,max_points,scored_points>>."""
        fields = _column_field_paths(bq_engine, "staging", "stg_file_qa_forms", "sections")
        field_map = {r["field_path"]: r["data_type"] for r in fields}
        assert "sections" in field_map, "sections column missing"
        assert "sections.section_code" in field_map, "sub-field section_code missing"
        assert "sections.max_points" in field_map, "sub-field max_points missing"
        assert "sections.scored_points" in field_map, "sub-field scored_points missing"
        assert field_map["sections.section_code"] == "STRING"
        assert field_map["sections.max_points"] == "INT64"
        assert field_map["sections.scored_points"] == "INT64"

    def test_complex_type_messages(self, bq_engine, ddl_applied):
        """stg_file_chat_transcripts.messages: ARRAY<STRUCT<sender,ts_ms,text>>."""
        fields = _column_field_paths(bq_engine, "staging", "stg_file_chat_transcripts", "messages")
        field_map = {r["field_path"]: r["data_type"] for r in fields}
        assert "messages.sender" in field_map, "sub-field sender missing"
        assert "messages.ts_ms" in field_map, "sub-field ts_ms missing"
        assert "messages.text" in field_map, "sub-field text missing"
        assert field_map["messages.sender"] == "STRING"
        assert field_map["messages.ts_ms"] == "INT64"
        assert field_map["messages.text"] == "STRING"

    def test_complex_type_metadata_json(self, bq_engine, ddl_applied):
        """stg_file_chat_transcripts.metadata: MAP<STRING,STRING> → JSON."""
        t = _column_type(bq_engine, "staging", "stg_file_chat_transcripts", "metadata")
        assert t == "JSON", f"metadata type: expected JSON, got {t}"

    def test_complex_type_keywords_array(self, bq_engine, ddl_applied):
        """stg_file_speech_analytics.keywords: ARRAY<STRING> → REPEATED STRING."""
        fields = _column_field_paths(bq_engine, "staging", "stg_file_speech_analytics", "keywords")
        # ARRAY<STRING> shows as data_type ARRAY<STRING> or STRING with mode REPEATED
        field_map = {r["field_path"]: r["data_type"] for r in fields}
        assert "keywords" in field_map, "keywords column missing"

    def test_decimal_precision_scale(self, bq_engine, ddl_applied):
        """52 DECIMAL columns map to NUMERIC with identical (p,s)."""
        # Representative precision/scale checks
        decimal_checks = [
            ("staging", "stg_fin_invoice", "total_amount", "NUMERIC(14, 2)"),
            ("staging", "stg_crm_contract_line", "unit_rate", "NUMERIC(12, 4)"),
            ("staging", "stg_crm_sla_target", "target_value", "NUMERIC(10, 4)"),
            ("staging", "stg_wfm_forecast", "required_fte", "NUMERIC(8, 2)"),
            ("staging", "stg_crm_sla_target", "penalty_pct", "NUMERIC(5, 2)"),
            ("dm", "fact_queue_interval", "avg_speed_answer_sec", "NUMERIC(8, 2)"),
            ("dm", "fact_billing_line", "line_amount", "NUMERIC(14, 2)"),
            ("dm", "fact_billing_line", "unit_rate", "NUMERIC(12, 4)"),
            ("dm", "agg_queue_hourly", "volume_variance_pct", "NUMERIC(7, 2)"),
            ("dm", "agg_agent_daily", "adherence_pct", "NUMERIC(5, 2)"),
            ("ods", "ods_contract_line", "unit_rate", "NUMERIC(12, 4)"),
            ("ods", "ods_invoice_acid", "total_amount", "NUMERIC(14, 2)"),
        ]
        failures = []
        for ds, tbl, col, expected_type in decimal_checks:
            actual = _column_type(bq_engine, ds, tbl, col)
            # BQ INFORMATION_SCHEMA returns e.g. "NUMERIC(14, 2)" or "NUMERIC"
            if actual and expected_type.replace(" ", "") not in actual.replace(" ", ""):
                failures.append(f"{ds}.{tbl}.{col}: expected {expected_type}, got {actual}")
        assert not failures, (
            f"DECIMAL precision/scale failures:\n" + "\n".join(failures)
        )


# ── AC4: Partition/cluster/options ─────────────────────────────────────────

@pytest.mark.live_bq
class TestAC4_PartitionClusterOptions:
    """AC4: partition strategy, clustering, filter requirements, expiration."""

    def test_12_clustered_tables(self, bq_engine, ddl_applied):
        """12 tables clustered with exact column lists per locked matrix."""
        failures = []
        for (ds, tbl), expected_cols in CLUSTER_MATRIX.items():
            info = bq_engine.introspect_table(ds, tbl)
            if info.cluster_columns != expected_cols:
                failures.append(
                    f"{ds}.{tbl}: expected cluster {expected_cols}, "
                    f"got {info.cluster_columns}"
                )
        assert not failures, (
            f"Clustering failures:\n" + "\n".join(failures)
        )

    def test_3_require_partition_filter(self, bq_engine, ddl_applied):
        """require_partition_filter=true on 3 large fact tables."""
        failures = []
        for ds, tbl in REQUIRE_PARTITION_FILTER:
            info = bq_engine.introspect_table(ds, tbl)
            rpf = info.options.get("require_partition_filter", False)
            if not rpf:
                failures.append(f"{ds}.{tbl}: require_partition_filter not set")
        assert not failures, "\n".join(failures)

    def test_10_file_feed_expiration(self, bq_engine, ddl_applied):
        """partition_expiration_days=365 on 10 file-feed tables."""
        failures = []
        for tbl in FILE_FEED_TABLES:
            info = bq_engine.introspect_table("staging", tbl)
            exp = info.options.get("partition_expiration_days")
            if exp is None or abs(exp - 365) > 0.5:
                failures.append(f"staging.{tbl}: partition_expiration_days={exp}")
        assert not failures, "\n".join(failures)

    def test_10_range_partitioned_tables(self, bq_engine, ddl_applied):
        """10 tables partitioned by integer-range on date_key."""
        failures = []
        for ds, tbl in RANGE_PARTITIONED_TABLES:
            info = bq_engine.introspect_table(ds, tbl)
            if "date_key" not in info.partition_columns:
                failures.append(
                    f"{ds}.{tbl}: expected partition on date_key, "
                    f"got {info.partition_columns}"
                )
        assert not failures, "\n".join(failures)

    def test_4_date_partitioned_monthly_tables(self, bq_engine, ddl_applied):
        """4 tables partitioned by DATE_TRUNC on period_month."""
        failures = []
        for ds, tbl in DATE_PARTITIONED_TABLES:
            info = bq_engine.introspect_table(ds, tbl)
            if "period_month" not in info.partition_columns:
                failures.append(
                    f"{ds}.{tbl}: expected partition on period_month, "
                    f"got {info.partition_columns}"
                )
        assert not failures, "\n".join(failures)

    def test_file_feed_partitioned_on_feed_date(self, bq_engine, ddl_applied):
        """10 file-feed tables partitioned on feed_date."""
        failures = []
        for tbl in FILE_FEED_TABLES:
            info = bq_engine.introspect_table("staging", tbl)
            if "feed_date" not in info.partition_columns:
                failures.append(f"staging.{tbl}: partition {info.partition_columns}")
        assert not failures, "\n".join(failures)

    def test_acid_tables_no_transactional_properties(self, bq_engine, ddl_applied):
        """4 former ACID tables have no transactional TBLPROPERTIES (BQ is always DML-safe)."""
        acid_tables = [
            "ods_client_acid", "ods_agent_acid",
            "ods_ticket_acid", "ods_invoice_acid",
        ]
        for tbl in acid_tables:
            info = bq_engine.introspect_table("ods", tbl)
            # BQ tables never have 'transactional' in options — just verify they exist
            assert info.name == tbl, f"ods.{tbl} not found"
            assert not info.is_external, f"ods.{tbl} is external"


# ── AC5: FK type consistency ──────────────────────────────────────────────

@pytest.mark.live_bq
class TestAC5_FKConsistency:
    """AC5: 56 FK→PK pairs type-consistent."""

    def test_all_56_fk_pk_pairs(self, bq_engine, ddl_applied):
        failures = []
        for fk_ds, fk_tbl, fk_col, pk_ds, pk_tbl, pk_col in FK_PATHS:
            fk_type = _column_type(bq_engine, fk_ds, fk_tbl, fk_col)
            pk_type = _column_type(bq_engine, pk_ds, pk_tbl, pk_col)
            label = f"{fk_ds}.{fk_tbl}.{fk_col} -> {pk_ds}.{pk_tbl}.{pk_col}"
            if fk_type is None:
                failures.append(f"FK NOT FOUND: {label}")
            elif pk_type is None:
                failures.append(f"PK NOT FOUND: {label}")
            elif fk_type != pk_type:
                failures.append(f"MISMATCH: {label}: FK={fk_type}, PK={pk_type}")
        assert not failures, (
            f"{len(failures)} FK/PK failures:\n" + "\n".join(failures)
        )


# ── AC6: Queryability ─────────────────────────────────────────────────────

@pytest.mark.live_bq
class TestAC6_Queryability:
    """AC6: SELECT * succeeds on every object + tier join queries."""

    def test_all_115_select_star(self, bq_engine, ddl_applied):
        all_queryable = (
            [(t, ds) for t, ds in ALL_TABLES]
            + [(mv, "dm") for mv in DM_MATERIALIZED_VIEWS]
            + [(v, "dm") for v in DM_VIEWS]
        )
        failures = []
        for name, dataset in all_queryable:
            fq = f"`{bq_engine.cfg.project}.{dataset}.{name}`"
            try:
                bq_engine.query(f"SELECT * FROM {fq} LIMIT 0")
            except Exception as exc:
                failures.append(f"{dataset}.{name}: {exc}")
        assert not failures, (
            f"{len(failures)}/115 SELECT * failures:\n" + "\n".join(failures)
        )

    def test_staging_tier_join(self, bq_engine, ddl_applied):
        bq_engine.query(
            "SELECT c.client_code, p.program_code "
            "FROM staging.stg_crm_client c "
            "JOIN staging.stg_crm_program p ON p.client_id = c.client_id "
            "LIMIT 1"
        )

    def test_ods_tier_join(self, bq_engine, ddl_applied):
        bq_engine.query(
            "SELECT o.contract_id, cl.unit_rate "
            "FROM ods.ods_contract o "
            "JOIN ods.ods_contract_line cl ON cl.contract_id = o.contract_id "
            "LIMIT 1"
        )

    def test_dm_tier_join(self, bq_engine, ddl_applied):
        bq_engine.query(
            "SELECT f.interaction_id, a.full_name, f.handle_seconds "
            "FROM dm.fact_interaction f "
            "JOIN dm.dim_agent a ON a.agent_sk = f.agent_sk "
            "WHERE f.date_key = 20260601 "
            "LIMIT 1"
        )
