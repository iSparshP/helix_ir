"""Benchmark: DDL compilation throughput."""

from __future__ import annotations

import time

import pyarrow as pa

from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


def make_wide_schema(n_cols: int = 100) -> Schema:
    """Create a wide schema with n_cols columns."""
    import random

    types = [
        pa.int64(), pa.float64(), pa.string(), pa.bool_(),
        pa.date32(), pa.timestamp("us"),
    ]
    fields = []
    for i in range(n_cols):
        t = types[i % len(types)]
        fields.append((f"col_{i}", HelixType(arrow_type=t, null_ratio=random.random() * 0.3)))
    return Schema(name="wide_table", fields=tuple(fields))


def bench_ddl_compilation(n_cols: int = 200, dialects: list[str] | None = None) -> None:
    from helix_ir.ddl import compile_ddl

    if dialects is None:
        dialects = ["duckdb", "postgres", "bigquery", "snowflake", "redshift", "databricks"]

    schema = make_wide_schema(n_cols)
    print(f"\nDDL compilation for {n_cols}-column schema:")

    for dialect in dialects:
        start = time.perf_counter()
        for _ in range(1000):
            script = compile_ddl(schema, dialect=dialect)
            _ = script.to_sql()
        elapsed = time.perf_counter() - start
        print(f"  {dialect:12s}: {elapsed:.3f}s for 1000 compilations ({1000/elapsed:.0f}/s)")


def bench_normalization(n_array_fields: int = 10) -> None:
    from helix_ir.normalize import normalize

    item_type = pa.struct([
        pa.field("sku", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("qty", pa.int64()),
    ])
    fields = [("id", HelixType(arrow_type=pa.int64()))]
    for i in range(n_array_fields):
        fields.append((f"items_{i}", HelixType(arrow_type=pa.list_(item_type))))

    schema = Schema(name="test", fields=tuple(fields))

    start = time.perf_counter()
    for _ in range(100):
        plan = normalize(schema, strategy="1nf")
    elapsed = time.perf_counter() - start
    print(f"\nNormalization ({n_array_fields} array fields): {elapsed:.3f}s for 100 runs")
    print(f"  Tables created: {len(plan.tables)}")
    print(f"  FKs: {len(plan.foreign_keys)}")


if __name__ == "__main__":
    print("=== Helix IR DDL Compilation Benchmarks ===")
    bench_ddl_compilation()
    bench_normalization()
