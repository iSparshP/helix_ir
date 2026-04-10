# helix-ir

[![PyPI version](https://img.shields.io/pypi/v/helix-ir.svg)](https://pypi.org/project/helix-ir/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python versions](https://img.shields.io/pypi/pyversions/helix-ir.svg)](https://pypi.org/project/helix-ir/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](tests/)

**helix-ir** is a comprehensive open-source Python library for schema inference, normalization, transformation, and DDL generation. It takes raw, messy document streams (JSON, Parquet, MongoDB, REST APIs, Kafka) and produces clean, typed, normalized schemas ready for any data warehouse — Postgres, Redshift, BigQuery, Snowflake, Databricks, or DuckDB.

## Installation

```bash
pip install helix-ir
# With optional extras:
pip install 'helix-ir[mongo]'       # MongoDB source
pip install 'helix-ir[postgres]'    # PostgreSQL source
pip install 'helix-ir[kafka]'       # Kafka source
pip install 'helix-ir[dev]'         # Development tools
```

## Hello World

```python
from helix_ir import infer, normalize, compile_ddl

# 1. Infer schema from documents
documents = [
    {"order_id": "ORD-001", "customer_email": "alice@example.com",
     "amount": 99.99, "items": [{"sku": "A1", "qty": 2}]},
    {"order_id": "ORD-002", "customer_email": "bob@example.com",
     "amount": 149.50, "items": [{"sku": "B3", "qty": 1}]},
]

schema = infer(documents, name="orders")
print(schema)
# Schema('orders', [order_id: string, customer_email: string, amount: double, items: list<...>])

# 2. Normalize to 1NF (flatten nested arrays)
plan = normalize(schema)
print(plan.table_names())
# ['orders', 'orders__items']

# 3. Compile DDL for your target warehouse
ddl = compile_ddl(plan, dialect="duckdb")
print(ddl)
# CREATE TABLE IF NOT EXISTS "orders" (
#   "__id" VARCHAR NOT NULL,
#   "order_id" VARCHAR NOT NULL,
#   ...
# );
# CREATE TABLE IF NOT EXISTS "orders__items" (
#   "__id" VARCHAR NOT NULL,
#   "__parent_id" VARCHAR NOT NULL,
#   ...
# );
```

## Quick Start: Infer + Normalize + DDL

```python
from helix_ir import infer, normalize, compile_ddl
from helix_ir.ddl import DDLOptions

# Infer with PII detection
schema = infer(
    documents,
    name="customers",
    sample_size=5000,
    seed=42,
    detect_pii=True,
    pii_locale="in",   # India locale: detects PAN, Aadhaar, GSTIN
)

# Check PII annotations
for name, ht in schema.fields:
    if ht.pii_class:
        print(f"  {name}: {ht.pii_class}")

# Normalize
plan = normalize(schema, strategy="1nf")

# Generate DDL with options
opts = DDLOptions(if_not_exists=True, schema_prefix="raw")
ddl_pg = compile_ddl(plan, dialect="postgres", options=opts)
ddl_bq = compile_ddl(plan, dialect="bigquery", options=opts)

print(ddl_pg.to_sql())
print(ddl_bq.to_sql())
```

## Quick Start: Transformation Pipeline

```python
from helix_ir.transform import Table
from helix_ir.transform.expression import col, lit

# Build a lazy query plan
table = Table("orders")
result = (
    table
    .filter(col("status") == lit("shipped"))
    .select(col("order_id"), col("amount"), col("customer_email"))
    .sort(col("amount").desc())
    .limit(100)
)

# Compile to SQL for any dialect
print(result.to_sql("duckdb"))
print(result.to_sql("bigquery"))
print(result.to_sql("snowflake"))
```

## Quick Start: Schema Diff

```python
from helix_ir.diff import diff

# Compare two schema versions
schema_v1 = infer(old_documents, name="orders")
schema_v2 = infer(new_documents, name="orders")

schema_diff = diff(schema_v1, schema_v2)

print(f"Breaking changes: {schema_diff.has_breaking_changes}")
for change in schema_diff.changes:
    print(f"  [{change.severity.upper()}] {change.description}")

# Filter by severity
breaking = schema_diff.filter("breaking")
```

## CLI

```bash
# Infer schema from a file
helix-ir infer data.ndjson --name orders --locale in

# Generate DDL
helix-ir ddl schema.json --dialect snowflake

# Normalize + DDL
helix-ir normalize schema.json --strategy 1nf --dialect bigquery

# Diff two schemas
helix-ir diff schema_v1.json schema_v2.json

# Generate data quality tests
helix-ir test schema.json --sensitivity 1.5

# Generate lineage graph
helix-ir lineage schema.json --format dot
```

## Module Overview

| Module | Description |
|---|---|
| `helix_ir.infer` | Schema inference from document streams using Algorithm R reservoir sampling |
| `helix_ir.types` | `HelixType` dataclass + type lattice (`join`, `meet`, `subsumes`) |
| `helix_ir.schema` | `Schema` class + `Path` addressing |
| `helix_ir.normalize` | 1NF / MongoDB / inline_small normalization strategies |
| `helix_ir.transform` | Lazy SQL transformation DSL with multi-dialect compilation |
| `helix_ir.ddl` | DDL generation for DuckDB, Postgres, Redshift, BigQuery, Snowflake, Databricks |
| `helix_ir.diff` | Schema diff with safe/risky/breaking change classification |
| `helix_ir.pii` | PII detection via field name heuristics + regex patterns (IN/US/EU) |
| `helix_ir.lineage` | Field-level lineage tracking with OpenLineage export |
| `helix_ir.sources` | Connectors: JSON, Parquet, MongoDB, PostgreSQL, REST, Kafka |
| `helix_ir.test` | Automatic data quality test generation from schema metadata |
| `helix_ir.cli` | Typer-based CLI with rich output |

## Documentation

Full documentation: https://helix-ir.readthedocs.io/en/latest/

## License

Apache 2.0 — Copyright 2026 Sparsh Prakash
