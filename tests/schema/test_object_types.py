"""AC3 — Object type verification: every source table lands as the correct target type.

98 of 100 source tables confirmed as TABLE in INFORMATION_SCHEMA.TABLES.
2 source aggregate tables (agg_site_daily, agg_agent_weekly) confirmed as
MATERIALIZED VIEW — intentional per locked Performance Optimization decision.
15 source views confirmed as VIEW in INFORMATION_SCHEMA.VIEWS.
Any silent type flip → HARD FAIL.
Pass: 115/115 object types correct, 2 intentional MV replacements documented,
      0 silent flips.
"""
from __future__ import annotations

import pytest

# Import canonical lists from test_catalog_presence (single source of truth).
from tests.schema.test_catalog_presence import (
    ALL_OBJECTS,
    DM_MATERIALIZED_VIEWS,
    DM_TABLES,
    DM_VIEWS,
    ODS_TABLES,
    STAGING_TABLES,
)


def _query_tables(bq_engine, dataset: str) -> dict[str, str]:
    """Read INFORMATION_SCHEMA.TABLES → {table_name: table_type}."""
    sql = (
        f"SELECT table_name, table_type "
        f"FROM `{bq_engine.cfg.project}.{dataset}.INFORMATION_SCHEMA.TABLES`"
    )
    return {r["table_name"]: r["table_type"] for r in bq_engine.query(sql)}


@pytest.mark.live_bq
class TestObjectTypes:
    """AC3: 115/115 object types correct, 2 intentional MV replacements, 0 flips."""

    def test_98_tables_are_base_table(self, bq_engine):
        """98 source tables confirmed as BASE TABLE (not VIEW or MV)."""
        all_catalogs = {}
        for ds in ("staging", "ods", "dm"):
            all_catalogs[ds] = _query_tables(bq_engine, ds)

        flips = []
        expected_tables = (
            [(t, "staging") for t in STAGING_TABLES]
            + [(t, "ods") for t in ODS_TABLES]
            + [(t, "dm") for t in DM_TABLES]
        )  # 98 total

        for name, dataset in expected_tables:
            actual = all_catalogs.get(dataset, {}).get(name)
            if actual is None:
                flips.append(f"MISSING: {dataset}.{name} (expected BASE TABLE)")
            elif actual != "BASE TABLE":
                flips.append(
                    f"SILENT TYPE FLIP: {dataset}.{name}: "
                    f"expected BASE TABLE, got {actual}"
                )

        assert len(expected_tables) == 98, f"Expected 98 tables, got {len(expected_tables)}"
        assert not flips, (
            f"{len(flips)} object type failures:\n" + "\n".join(flips)
        )

    def test_2_mvs_are_materialized_view(self, bq_engine):
        """2 source agg tables (agg_site_daily, agg_agent_weekly) are MATERIALIZED VIEW.
        Intentional per locked Performance Optimization decision."""
        catalog = _query_tables(bq_engine, "dm")

        for mv_name in DM_MATERIALIZED_VIEWS:
            actual = catalog.get(mv_name)
            assert actual == "MATERIALIZED VIEW", (
                f"dm.{mv_name}: expected MATERIALIZED VIEW, got {actual}"
            )

    def test_2_mv_replacements_documented(self, bq_engine):
        """Document the 2 intentional type changes (TABLE → MV)."""
        replacements = {
            "mv_agent_weekly": "Replaces agg_agent_weekly (TABLE → MV)",
            "mv_site_daily": "Replaces agg_site_daily (TABLE → MV)",
        }
        catalog = _query_tables(bq_engine, "dm")

        for mv_name, reason in replacements.items():
            actual = catalog.get(mv_name)
            assert actual == "MATERIALIZED VIEW", (
                f"dm.{mv_name}: expected MATERIALIZED VIEW for "
                f"intentional replacement ({reason}), got {actual}"
            )
        # Also verify the original table names do NOT exist as tables
        assert "agg_agent_weekly" not in catalog, (
            "agg_agent_weekly should not exist as a table — replaced by mv_agent_weekly"
        )
        assert "agg_site_daily" not in catalog, (
            "agg_site_daily should not exist as a table — replaced by mv_site_daily"
        )

    def test_15_views_are_view(self, bq_engine):
        """15 source views confirmed as VIEW in INFORMATION_SCHEMA."""
        catalog = _query_tables(bq_engine, "dm")

        flips = []
        for name in DM_VIEWS:
            actual = catalog.get(name)
            if actual is None:
                flips.append(f"MISSING: dm.{name} (expected VIEW)")
            elif actual != "VIEW":
                flips.append(
                    f"SILENT TYPE FLIP: dm.{name}: expected VIEW, got {actual}"
                )

        assert not flips, (
            f"{len(flips)} view type failures:\n" + "\n".join(flips)
        )

    def test_no_silent_type_flips(self, bq_engine):
        """Comprehensive check: every object has its expected type, no exceptions."""
        all_catalogs = {}
        for ds in ("staging", "ods", "dm"):
            all_catalogs[ds] = _query_tables(bq_engine, ds)

        flips = []
        for name, dataset, expected_type in ALL_OBJECTS:
            actual = all_catalogs.get(dataset, {}).get(name)
            if actual is None:
                flips.append(f"MISSING: {dataset}.{name} (expected {expected_type})")
            elif actual != expected_type:
                flips.append(
                    f"TYPE FLIP: {dataset}.{name}: "
                    f"expected {expected_type}, got {actual}"
                )

        assert not flips, (
            f"{len(flips)} type flips detected (0 expected):\n"
            + "\n".join(flips)
        )

    def test_total_115_objects(self):
        """Sanity: 98 tables + 2 MVs + 15 views = 115."""
        assert len(ALL_OBJECTS) == 115
