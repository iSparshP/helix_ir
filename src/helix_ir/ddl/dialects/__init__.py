"""DDL dialect registry."""

from __future__ import annotations

from helix_ir.ddl.dialects.base import BaseDialect, DDLOptions, DDLScript
from helix_ir.ddl.dialects.bigquery import BigQueryDialect
from helix_ir.ddl.dialects.databricks import DatabricksDialect
from helix_ir.ddl.dialects.duckdb import DuckDBDialect
from helix_ir.ddl.dialects.postgres import PostgresDialect
from helix_ir.ddl.dialects.redshift import RedshiftDialect
from helix_ir.ddl.dialects.snowflake import SnowflakeDialect

DIALECTS: dict[str, type[BaseDialect]] = {
    "duckdb": DuckDBDialect,
    "postgres": PostgresDialect,
    "postgresql": PostgresDialect,
    "redshift": RedshiftDialect,
    "bigquery": BigQueryDialect,
    "snowflake": SnowflakeDialect,
    "databricks": DatabricksDialect,
}


def get_dialect(name: str) -> BaseDialect:
    """Return a dialect instance by name."""
    name_lower = name.lower()
    cls = DIALECTS.get(name_lower)
    if cls is None:
        available = ", ".join(sorted(DIALECTS.keys()))
        raise ValueError(
            f"Unknown dialect {name!r}. Available dialects: {available}"
        )
    return cls()


__all__ = [
    "BaseDialect",
    "DDLOptions",
    "DDLScript",
    "DuckDBDialect",
    "PostgresDialect",
    "RedshiftDialect",
    "BigQueryDialect",
    "SnowflakeDialect",
    "DatabricksDialect",
    "DIALECTS",
    "get_dialect",
]
