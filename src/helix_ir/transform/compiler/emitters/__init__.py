"""SQL emitter registry."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter
from helix_ir.transform.compiler.emitters.bigquery import BigQueryEmitter
from helix_ir.transform.compiler.emitters.databricks import DatabricksEmitter
from helix_ir.transform.compiler.emitters.duckdb import DuckDBEmitter
from helix_ir.transform.compiler.emitters.postgres import PostgresEmitter
from helix_ir.transform.compiler.emitters.redshift import RedshiftEmitter
from helix_ir.transform.compiler.emitters.snowflake import SnowflakeEmitter

EMITTERS: dict[str, type[BaseEmitter]] = {
    "duckdb": DuckDBEmitter,
    "postgres": PostgresEmitter,
    "postgresql": PostgresEmitter,
    "redshift": RedshiftEmitter,
    "bigquery": BigQueryEmitter,
    "snowflake": SnowflakeEmitter,
    "databricks": DatabricksEmitter,
}


def get_emitter(dialect: str) -> BaseEmitter:
    """Return an emitter instance by dialect name."""
    cls = EMITTERS.get(dialect.lower())
    if cls is None:
        raise ValueError(f"Unknown emitter dialect: {dialect!r}")
    return cls()


__all__ = [
    "BaseEmitter",
    "DuckDBEmitter",
    "PostgresEmitter",
    "RedshiftEmitter",
    "BigQueryEmitter",
    "SnowflakeEmitter",
    "DatabricksEmitter",
    "EMITTERS",
    "get_emitter",
]
