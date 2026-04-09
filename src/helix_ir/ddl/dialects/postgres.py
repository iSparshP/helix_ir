"""PostgreSQL DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect


class PostgresDialect(BaseDialect):
    """PostgreSQL DDL dialect."""

    DIALECT_NAME = "postgres"

    TYPE_MAP: dict[str, str] = {
        "null": "VARCHAR",
        "bool": "BOOLEAN",
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
        "float16": "REAL",
        "float32": "REAL",
        "float64": "DOUBLE PRECISION",
        "string": "VARCHAR",
        "large_string": "TEXT",
        "binary": "BYTEA",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        if t.tz:
            return "TIMESTAMPTZ"
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "JSONB"

    def _list_type(self, t: pa.ListType) -> str:
        inner = self.arrow_to_sql_type(t.value_type)
        return f"{inner}[]"

    def _struct_type(self, t: pa.StructType) -> str:
        # PostgreSQL doesn't have native struct type; use JSONB
        return "JSONB"
