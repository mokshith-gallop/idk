#!/usr/bin/env python3
"""Generate the nbcs_physical_schema.mvs.yaml from the authored DDL files
and the source Hive DDL files.

Reads:
  - /workspace/project/sql/ddl/0*.sql   (target BQ DDL)
  - /workspace/source/hive/ddl/0*.hql   (source Hive DDL)

Writes:
  - /workspace/project/sql/nbcs_physical_schema.mvs.yaml
"""
import re, sys, os, textwrap
from collections import OrderedDict

# ---------------------------------------------------------------------------
# Parse BQ DDL files
# ---------------------------------------------------------------------------

def parse_bq_ddl(filepath):
    """Parse a BQ DDL file. Returns list of table dicts."""
    with open(filepath) as f:
        content = f.read()

    tables = []
    # Match CREATE TABLE / CREATE MATERIALIZED VIEW / CREATE VIEW
    pattern = r'CREATE\s+(TABLE|MATERIALIZED\s+VIEW|VIEW)\s+IF\s+NOT\s+EXISTS\s+(\w+\.\w+)\s*(?:\(|AS)'
    
    # Split on CREATE statements
    parts = re.split(r'(?=CREATE\s+(?:TABLE|MATERIALIZED\s+VIEW|VIEW)\s+IF\s+NOT\s+EXISTS)', content)
    
    for part in parts:
        if not part.strip():
            continue
        m = re.match(r'CREATE\s+(TABLE|MATERIALIZED\s+VIEW|VIEW)\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)', part)
        if not m:
            continue
        
        obj_type_raw = m.group(1).strip()
        dataset = m.group(2)
        name = m.group(3)
        
        if obj_type_raw == 'TABLE':
            obj_type = 'TABLE'
        elif 'MATERIALIZED' in obj_type_raw:
            obj_type = 'MATERIALIZED_VIEW'
        else:
            obj_type = 'VIEW'
        
        table = {
            'dataset': dataset,
            'name': name,
            'object_type': obj_type,
            'columns': [],
            'partition_by': None,
            'cluster_by': None,
            'options': {},
        }
        
        if obj_type == 'VIEW':
            # Views don't have column definitions in DDL
            tables.append(table)
            continue
        
        if obj_type == 'MATERIALIZED_VIEW':
            # MV columns come from the SELECT
            tables.append(table)
            continue
        
        # Parse columns from CREATE TABLE
        col_section = re.search(r'\((.*?)\)\s*(?:PARTITION|CLUSTER|OPTIONS|;)', part, re.DOTALL)
        if not col_section:
            tables.append(table)
            continue
        
        col_text = col_section.group(1)
        for line in col_text.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):
                continue
            # Match column definition
            cm = re.match(r'(\w+)\s+(INT64|STRING|BOOL|FLOAT64|TIMESTAMP|DATE|JSON|NUMERIC\(\d+,\d+\)|ARRAY<[^>]+(?:>[^>]*)*>)', line)
            if cm:
                col_name = cm.group(1)
                col_type = cm.group(2)
                # Extract description
                desc_match = re.search(r"OPTIONS\(description='([^']+)'\)", line)
                desc = desc_match.group(1) if desc_match else None
                
                # Extract scale for NUMERIC
                scale = None
                nm = re.match(r'NUMERIC\((\d+),(\d+)\)', col_type)
                if nm:
                    scale = int(nm.group(2))
                
                table['columns'].append({
                    'name': col_name,
                    'type': col_type,
                    'description': desc,
                    'scale': scale,
                })
        
        # Parse PARTITION BY
        pm = re.search(r'PARTITION\s+BY\s+(.*?)(?:CLUSTER|OPTIONS|;)', part, re.DOTALL)
        if pm:
            table['partition_by'] = pm.group(1).strip().rstrip(';').strip()
        
        # Parse CLUSTER BY
        cm2 = re.search(r'CLUSTER\s+BY\s+(.*?)(?:OPTIONS|;|\n)', part)
        if cm2:
            table['cluster_by'] = [c.strip().rstrip(';') for c in cm2.group(1).split(',')]
        
        # Parse OPTIONS
        om = re.search(r'OPTIONS\(\s*((?:require_partition_filter|partition_expiration_days)\s*=\s*\w+(?:\s*,\s*(?:require_partition_filter|partition_expiration_days)\s*=\s*\w+)*)\s*\)', part)
        if om:
            for opt in om.group(1).split(','):
                k, v = opt.strip().split('=')
                k = k.strip()
                v = v.strip()
                if v == 'true':
                    v = True
                elif v == 'false':
                    v = False
                elif v.isdigit():
                    v = int(v)
                table['options'][k] = v
        
        tables.append(table)
    
    return tables


def parse_hive_ddl(filepath):
    """Parse a Hive DDL file. Returns list of table dicts with source types."""
    with open(filepath) as f:
        content = f.read()

    tables = []
    parts = re.split(r'(?=CREATE\s+(?:EXTERNAL\s+)?TABLE\s+IF\s+NOT\s+EXISTS)', content)
    
    for part in parts:
        if not part.strip():
            continue
        m = re.match(r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)\.(\w+)', part)
        if not m:
            continue
        
        dataset = m.group(1)
        name = m.group(2)
        
        table = {
            'dataset': dataset,
            'name': name,
            'columns': [],
            'partition_cols': [],
        }
        
        # Extract column section (before PARTITIONED BY or STORED AS or CLUSTERED BY)
        col_section = re.search(r'\((.*?)\)\s*(?:PARTITIONED|STORED|CLUSTERED|ROW\s+FORMAT)', part, re.DOTALL)
        if col_section:
            for line in col_section.group(1).split('\n'):
                line = line.strip().rstrip(',')
                if not line or line.startswith('--'):
                    continue
                # Handle complex types in mapping form
                cm = re.match(r'(\w+)\s+(BIGINT|INT|SMALLINT|STRING|BOOLEAN|DOUBLE|TIMESTAMP|DATE|DECIMAL\(\d+,\d+\)|ARRAY<[^>]+(?:>[^>]*)*>|MAP<[^>]+>)', line)
                if cm:
                    col_name = cm.group(1)
                    col_type = cm.group(2)
                    # Extract COMMENT
                    desc_match = re.search(r"COMMENT\s+'([^']+)'", line)
                    desc = desc_match.group(1) if desc_match else None
                    table['columns'].append({
                        'name': col_name,
                        'type': col_type,
                        'description': desc,
                    })
        
        # Extract partition columns
        pm = re.search(r'PARTITIONED\s+BY\s+\(([^)]+)\)', part)
        if pm:
            for pc in pm.group(1).split(','):
                pc = pc.strip()
                pcm = re.match(r'(\w+)\s+(\w+)', pc)
                if pcm:
                    table['partition_cols'].append({
                        'name': pcm.group(1),
                        'type': pcm.group(2),
                    })
        
        tables.append(table)
    
    return tables


# ---------------------------------------------------------------------------
# Type mapping helpers
# ---------------------------------------------------------------------------

HIVE_TO_BQ = {
    'BIGINT': 'INT64',
    'INT': 'INT64',
    'SMALLINT': 'INT64',
    'STRING': 'STRING',
    'BOOLEAN': 'BOOL',
    'DOUBLE': 'FLOAT64',
    'TIMESTAMP': 'TIMESTAMP',
    'DATE': 'DATE',
}

def hive_type_to_bq(hive_type):
    if hive_type in HIVE_TO_BQ:
        return HIVE_TO_BQ[hive_type]
    m = re.match(r'DECIMAL\((\d+),(\d+)\)', hive_type)
    if m:
        return f'NUMERIC({m.group(1)},{m.group(2)})'
    if hive_type.startswith('ARRAY<STRUCT'):
        # Translate inner types
        result = hive_type
        result = result.replace(':STRING', ' STRING').replace(':INT', ' INT64').replace(':BIGINT', ' INT64')
        result = re.sub(r'(?<!\w)INT(?!\d)', 'INT64', result)
        return result
    if hive_type.startswith('ARRAY<STRING>'):
        return 'ARRAY<STRING>'
    if hive_type.startswith('MAP<'):
        return 'JSON'
    return hive_type


# ---------------------------------------------------------------------------
# MV column definitions (hardcoded from the SELECT)
# ---------------------------------------------------------------------------

MV_COLUMNS = {
    'mv_agent_weekly': [
        {'name': 'week_start_key', 'type': 'INT64', 'source_type': 'INT', 'source_name': 'week_start_key'},
        {'name': 'agent_sk', 'type': 'INT64', 'source_type': 'BIGINT', 'source_name': 'agent_sk'},
        {'name': 'site_code', 'type': 'STRING', 'source_type': 'STRING', 'source_name': 'site_code'},
        {'name': 'days_worked', 'type': 'INT64', 'source_type': 'INT', 'source_name': 'days_worked'},
        {'name': 'interactions_handled', 'type': 'INT64', 'source_type': 'INT', 'source_name': 'interactions_handled'},
        {'name': 'avg_handle_seconds', 'type': 'NUMERIC', 'source_type': 'DECIMAL(8,2)', 'source_name': 'avg_handle_seconds', 'scale': 9},
        {'name': 'adherence_pct', 'type': 'NUMERIC', 'source_type': 'DECIMAL(5,2)', 'source_name': 'adherence_pct', 'scale': 9},
        {'name': 'occupancy_pct', 'type': 'NUMERIC', 'source_type': 'DECIMAL(5,2)', 'source_name': 'occupancy_pct', 'scale': 9},
    ],
    'mv_site_daily': [
        {'name': 'date_key', 'type': 'INT64', 'source_type': 'INT', 'source_name': 'date_key'},
        {'name': 'site_code', 'type': 'STRING', 'source_type': 'STRING', 'source_name': 'site_code'},
        {'name': 'agents_active', 'type': 'INT64', 'source_type': 'INT', 'source_name': 'agents_active'},
        {'name': 'interactions', 'type': 'INT64', 'source_type': 'BIGINT', 'source_name': 'interactions'},
        {'name': 'avg_handle_seconds', 'type': 'NUMERIC', 'source_type': 'DECIMAL(8,2)', 'source_name': 'avg_handle_seconds', 'scale': 9},
        {'name': 'sl_pct', 'type': 'NUMERIC', 'source_type': 'DECIMAL(5,2)', 'source_name': 'sl_pct', 'scale': 9},
        {'name': 'adherence_pct', 'type': 'NUMERIC', 'source_type': 'DECIMAL(5,2)', 'source_name': 'adherence_pct', 'scale': 9},
    ],
}

# Source table mapping for MVs
MV_SOURCE = {
    'mv_agent_weekly': 'agg_agent_weekly',
    'mv_site_daily': 'agg_site_daily',
}

# ---------------------------------------------------------------------------
# View list (15 views)
# ---------------------------------------------------------------------------

VIEWS = [
    'vw_org_hierarchy', 'vw_active_agents_ndv', 'vw_csat_rollup',
    'vw_call_driver_regex', 'vw_repeat_contact_window',
    'vw_billing_reconciliation', 'vw_agent_roster_current',
    'vw_agent_scorecard', 'vw_attrition_risk', 'vw_queue_sla_attainment',
    'vw_first_contact_resolution', 'vw_occupancy_utilization',
    'vw_shrinkage_analysis', 'vw_program_margin',
    'vw_client_executive_summary',
]


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------

def main():
    # Parse all BQ DDL files
    bq_files = [
        '/workspace/project/sql/ddl/02-staging-sqoop-mirrors.sql',
        '/workspace/project/sql/ddl/03-staging-delta-feeds.sql',
        '/workspace/project/sql/ddl/04-staging-file-feeds.sql',
        '/workspace/project/sql/ddl/05-ods-cleanse.sql',
        '/workspace/project/sql/ddl/06-ods-delta-scd2.sql',
        '/workspace/project/sql/ddl/07-ods-acid.sql',
        '/workspace/project/sql/ddl/08-dm-tables.sql',
    ]
    
    hive_files = [
        '/workspace/source/hive/ddl/02-staging-sqoop-mirrors.hql',
        '/workspace/source/hive/ddl/03-staging-delta-feeds.hql',
        '/workspace/source/hive/ddl/04-staging-file-feeds.hql',
        '/workspace/source/hive/ddl/05-ods-cleanse.hql',
        '/workspace/source/hive/ddl/06-ods-delta-scd2.hql',
        '/workspace/source/hive/ddl/07-ods-acid.hql',
        '/workspace/source/hive/ddl/08-dm-tables.hql',
    ]
    
    bq_tables = []
    for f in bq_files:
        bq_tables.extend(parse_bq_ddl(f))
    
    hive_tables = []
    for f in hive_files:
        hive_tables.extend(parse_hive_ddl(f))
    
    # Build lookup by name
    hive_lookup = {}
    for ht in hive_tables:
        hive_lookup[ht['name']] = ht
    
    # Group BQ tables by dataset
    by_dataset = {'staging': [], 'ods': [], 'dm': []}
    for bt in bq_tables:
        by_dataset[bt['dataset']].append(bt)
    
    # Count columns
    total_cols = sum(len(t['columns']) for t in bq_tables)
    # Add MV columns
    for mv_name, mv_cols in MV_COLUMNS.items():
        total_cols += len(mv_cols)
    
    print(f"Total table columns: {total_cols}", file=sys.stderr)
    print(f"Total tables: {len(bq_tables)}", file=sys.stderr)
    
    # Now generate YAML
    lines = []
    lines.append("# =============================================================================")
    lines.append("# nbcs_physical_schema.mvs.yaml")
    lines.append("# Migration Validation Spec — BigQuery Physical Schema DDL")
    lines.append("#")
    lines.append("# Mode: build-and-verify (default)")
    lines.append("# Applies 10 DDL files to a clean build dataset, then verifies the built")
    lines.append("# result against source ground truth (manifests/tables.yaml + hive/ddl/*.hql).")
    lines.append("#")
    lines.append("# Objects: 115 (3 schemas + 98 tables + 2 MVs + 15 views)")
    lines.append(f"# Columns: {total_cols} across 100 data objects (98 tables + 2 MVs)")
    lines.append("# =============================================================================")
    lines.append("")
    lines.append("name: nbcs_physical_schema")
    lines.append("")
    lines.append("connections:")
    lines.append("  source: { engine: impala }")
    lines.append("  target: { engine: bigquery }")
    lines.append("")
    
    # Migration block
    lines.append("migration:")
    lines.append("  source_map:")
    
    # Build source_map from all tables + MVs
    for ds in ['staging', 'ods', 'dm']:
        for bt in by_dataset[ds]:
            src_name = bt['name']
            tgt_name = bt['name']
            src_db = '${SOURCE_DATABASE}'
            lines.append(f'    - {{ source: "{src_db}.{src_name}", target: {tgt_name} }}')
    
    # Add MV source mappings
    for mv_name, src_name in MV_SOURCE.items():
        lines.append(f'    - {{ source: "${{SOURCE_DATABASE}}.{src_name}", target: {mv_name} }}')
    
    lines.append("")
    lines.append("  steps:")
    lines.append('    - { kind: ddl, sql: sql/ddl/01-create-datasets.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/02-staging-sqoop-mirrors.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/03-staging-delta-feeds.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/04-staging-file-feeds.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/05-ods-cleanse.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/06-ods-delta-scd2.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/07-ods-acid.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/08-dm-tables.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/08b-dm-materialized-views.sql }')
    lines.append('    - { kind: ddl, sql: sql/ddl/09-dm-views.sql }')
    lines.append("")
    
    # Suites section
    lines.append("suites:")
    
    # Generate one schema_conformance suite per dataset
    for ds_name, ds_label, expect_count in [
        ('staging', 'staging', 45),
        ('ods', 'ods', 30),
        ('dm', 'dm', 25),  # 23 tables + 2 MVs
    ]:
        lines.append(f"  # {'='*70}")
        lines.append(f"  # {ds_label} dataset — schema conformance")
        lines.append(f"  # {'='*70}")
        lines.append(f"  - pattern: schema_conformance")
        lines.append(f"    id: schema-{ds_name}")
        lines.append(f"    target_dataset: \"${{BUILD_DATASET}}_{ds_name}\"")
        lines.append(f"    source_database: \"${{SOURCE_DATABASE}}\"")
        lines.append(f"    expect_table_count: {expect_count}")
        lines.append(f"    tables:")
        
        for bt in by_dataset[ds_name]:
            tname = bt['name']
            hive_t = hive_lookup.get(tname)
            
            # Source table name (for MVs, map to original)
            src_table = MV_SOURCE.get(tname, tname)
            hive_t = hive_lookup.get(src_table)
            
            lines.append(f"      - table: {tname}")
            lines.append(f"        source_table: {src_table}")
            lines.append(f"        expect_object_type: {bt['object_type']}")
            
            # Partition
            if bt['partition_by']:
                lines.append(f"        partition_by: \"{bt['partition_by']}\"")
            
            # Cluster
            if bt['cluster_by']:
                cluster_str = ', '.join(bt['cluster_by'])
                lines.append(f"        cluster_by: [{cluster_str}]")
            
            # Table options
            if bt['options']:
                lines.append(f"        table_options:")
                for k, v in bt['options'].items():
                    if isinstance(v, bool):
                        lines.append(f"          {k}: {'true' if v else 'false'}")
                    else:
                        lines.append(f"          {k}: {v}")
            
            # Columns
            if bt['columns']:
                lines.append(f"        columns:")
                for col in bt['columns']:
                    parts = [f"name: {col['name']}", f"type: {col['type']}"]
                    
                    # Scale for NUMERIC
                    if col.get('scale') is not None:
                        parts.append(f"scale: {col['scale']}")
                    
                    # Source type mapping
                    if hive_t:
                        # Find matching source column
                        src_col = None
                        for sc in hive_t['columns']:
                            if sc['name'] == col['name']:
                                src_col = sc
                                break
                        # Also check partition columns
                        if not src_col:
                            for pc in hive_t.get('partition_cols', []):
                                if pc['name'] == col['name']:
                                    src_col = {'name': pc['name'], 'type': pc['type'], 'description': None}
                                    break
                        
                        if src_col:
                            parts.append(f"source_type: {src_col['type']}")
                            if src_col.get('description'):
                                desc = src_col['description']
                                parts.append(f"description: \"{desc}\"")
                    
                    line = '          - { ' + ', '.join(parts) + ' }'
                    lines.append(line)
        
        # Add MVs for dm dataset
        if ds_name == 'dm':
            for mv_name, mv_cols in MV_COLUMNS.items():
                src_table = MV_SOURCE[mv_name]
                lines.append(f"      - table: {mv_name}")
                lines.append(f"        source_table: {src_table}")
                lines.append(f"        expect_object_type: MATERIALIZED_VIEW")
                lines.append(f"        columns:")
                for col in mv_cols:
                    parts = [f"name: {col['name']}", f"type: {col['type']}"]
                    if col.get('scale') is not None:
                        parts.append(f"scale: {col['scale']}")
                    parts.append(f"source_type: {col['source_type']}")
                    line = '          - { ' + ', '.join(parts) + ' }'
                    lines.append(line)
            
            # Add views
            for vname in VIEWS:
                lines.append(f"      - table: {vname}")
                lines.append(f"        expect_object_type: VIEW")
    
    lines.append("")
    
    # Write output
    output_path = '/workspace/project/sql/nbcs_physical_schema.mvs.yaml'
    with open(output_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    
    print(f"Written to {output_path}", file=sys.stderr)
    print(f"Total lines: {len(lines)}", file=sys.stderr)


if __name__ == '__main__':
    main()
