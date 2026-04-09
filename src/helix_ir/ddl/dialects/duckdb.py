"""DuckDB DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect
from helix_ir.types.core import HelixType


class DuckDBDialect(BaseDialect):
    """DuckDB DDL dialect — closest to Arrow types."""

    DIALECT_NAME = "duckdb"

    TYPE_MAP: dict[str, str] = {
        "null": "VARCHAR",
        "bool": "BOOLEAN",
        "int8": "TINYINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "UTINYINT",
        "uint16": "USMALLINT",
        "uint32": "UINTEGER",
        "uint64": "UBIGINT",
        "float16": "FLOAT",
        "float32": "FLOAT",
        "float64": "DOUBLE",
        "string": "VARCHAR",
        "large_string": "VARCHAR",
        "binary": "BLOB",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "JSON"

    def _list_type(self, t: pa.ListType) -> str:
        inner = self.arrow_to_sql_type(t.value_type)
        return f"{inner}[]"

    def _struct_type(self, t: pa.StructType) -> str:
        if t.num_fields == 0:
            return "STRUCT()"
        fields = ", ".join(
            f'"{t.field(i).name}" {self.arrow_to_sql_type(t.field(i).type)}'
            for i in range(t.num_fields)
        )
        return f"STRUCT({fields})"
