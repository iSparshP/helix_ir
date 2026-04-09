"""Snowflake DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect
from helix_ir.types.core import HelixType


class SnowflakeDialect(BaseDialect):
    """Snowflake DDL dialect."""

    DIALECT_NAME = "snowflake"

    TYPE_MAP: dict[str, str] = {
        "null": "VARCHAR",
        "bool": "BOOLEAN",
        "int8": "NUMBER(3,0)",
        "int16": "NUMBER(5,0)",
        "int32": "NUMBER(10,0)",
        "int64": "NUMBER(19,0)",
        "uint8": "NUMBER(3,0)",
        "uint16": "NUMBER(5,0)",
        "uint32": "NUMBER(10,0)",
        "uint64": "NUMBER(20,0)",
        "float16": "FLOAT",
        "float32": "FLOAT",
        "float64": "FLOAT",
        "string": "VARCHAR",
        "large_string": "VARCHAR",
        "binary": "BINARY",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        if t.tz:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP_NTZ"

    def _json_type(self) -> str:
        return "VARIANT"

    def _list_type(self, t: pa.ListType) -> str:
        return "ARRAY"

    def _struct_type(self, t: pa.StructType) -> str:
        return "OBJECT"

    def _column_def(self, name: str, ht: HelixType) -> str:
        sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
        nullable = "" if ht.null_ratio > 0 else " NOT NULL"
        col = f"{self.quote_identifier(name)} {sql_type}{nullable}"
        if ht.description:
            col += f" COMMENT '{ht.description}'"
        return col
