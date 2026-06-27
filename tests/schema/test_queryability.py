"""AC6 — Queryability: SELECT * succeeds on every data object and view.

Executes SELECT * LIMIT 0 on each of 98 tables, 2 MVs, and 15 views (115 total).
Then runs one representative cross-table join query per dataset tier.
Any failure names the object and BQ's actual error → HARD FAIL.

Catalog metadata matching alone is NOT sufficient — this test proves the objects
are readable end-to-end.
"""
from __future__ import annotations

import pytest

from tests.schema.test_catalog_presence import (
    ALL_TABLES,
    DM_MATERIALIZED_VIEWS,
    DM_VIEWS,
)


def _select_star(bq_engine, dataset: str, table: str) -> str | None:
    """Execute SELECT * LIMIT 0.  Return None on success, error message on failure."""
    fq = f"`{bq_engine.cfg.project}.{dataset}.{table}`"
    try:
        bq_engine.query(f"SELECT * FROM {fq} LIMIT 0")
        return None
    except Exception as exc:
        return str(exc)


# ---------------------------------------------------------------------------
# AC6 — 115 objects queryable
# ---------------------------------------------------------------------------


@pytest.mark.live_bq
class TestQueryability:
    """AC6: SELECT * LIMIT 0 succeeds on every object; tier join queries pass."""

    def test_all_98_tables(self, bq_engine):
        """SELECT * LIMIT 0 on each of 98 BASE TABLEs — 0 errors."""
        failures = []
        for name, dataset in ALL_TABLES:
            err = _select_star(bq_engine, dataset, name)
            if err is not None:
                failures.append(f"FAIL: {dataset}.{name}: {err}")
        assert not failures, (
            f"{len(failures)}/98 tables failed SELECT *:\n" + "\n".join(failures)
        )

    def test_2_materialized_views(self, bq_engine):
        """SELECT * LIMIT 0 on each of 2 materialized views — 0 errors."""
        failures = []
        for mv in DM_MATERIALIZED_VIEWS:
            err = _select_star(bq_engine, "dm", mv)
            if err is not None:
                failures.append(f"FAIL: dm.{mv}: {err}")
        assert not failures, (
            f"{len(failures)}/2 MVs failed SELECT *:\n" + "\n".join(failures)
        )

    def test_15_views(self, bq_engine):
        """SELECT * LIMIT 0 on each of 15 views — 0 errors.

        This is the authoritative readability check — catalog presence alone
        does NOT prove the view SQL resolves without errors.
        """
        failures = []
        for v in DM_VIEWS:
            err = _select_star(bq_engine, "dm", v)
            if err is not None:
                failures.append(f"FAIL: dm.{v}: {err}")
        assert not failures, (
            f"{len(failures)}/15 views failed SELECT *:\n" + "\n".join(failures)
        )

    # ── Tier-specific join queries ──────────────────────────────────────

    def test_staging_tier_join(self, bq_engine):
        """Staging cross-table join — 0 type-coercion errors."""
        sql = (
            "SELECT c.client_code, p.program_code "
            "FROM staging.stg_crm_client c "
            "JOIN staging.stg_crm_program p ON p.client_id = c.client_id "
            "LIMIT 1"
        )
        # Must not raise — any type-coercion or resolution error is a FAIL.
        bq_engine.query(sql)

    def test_ods_tier_join(self, bq_engine):
        """ODS cross-table join — 0 type-coercion errors."""
        sql = (
            "SELECT o.contract_id, cl.unit_rate "
            "FROM ods.ods_contract o "
            "JOIN ods.ods_contract_line cl ON cl.contract_id = o.contract_id "
            "LIMIT 1"
        )
        bq_engine.query(sql)

    def test_dm_tier_join(self, bq_engine):
        """DM cross-table join with partition filter — 0 type-coercion errors."""
        sql = (
            "SELECT f.interaction_id, a.full_name, f.handle_seconds "
            "FROM dm.fact_interaction f "
            "JOIN dm.dim_agent a ON a.agent_sk = f.agent_sk "
            "WHERE f.date_key = 20260601 "
            "LIMIT 1"
        )
        bq_engine.query(sql)
