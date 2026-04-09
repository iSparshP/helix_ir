"""Amazon Redshift DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect, DDLOptions
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


class RedshiftDialect(BaseDialect):
    """Amazon Redshift DDL dialect."""

    DIALECT_NAME = "redshift"

    TYPE_MAP: dict[str, str] = {
        "null": "VARCHAR(65535)",
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
        "string": "VARCHAR(65535)",
        "large_string": "VARCHAR(65535)",
        "binary": "VARBYTE",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "SUPER"

    def _list_type(self, t: pa.ListType) -> str:
        return "SUPER"

    def _struct_type(self, t: pa.StructType) -> str:
        return "SUPER"

    def compile_create_table(
        self,
        schema: Schema,
        options: DDLOptions,
    ) -> str:
        """Generate a CREATE TABLE statement with Redshift-specific hints."""
        base = super().compile_create_table(schema, options)

        # Add ENCODE hints as a comment
        encode_hints: list[str] = []
        for fname, ht in schema.fields:
            if pa.types.is_string(ht.arrow_type):
                encode_hints.append(f"  ENCODE ZSTD ({self.quote_identifier(fname)})")

        hints = options.extra_options
        distkey = hints.get("distkey")
        sortkey = hints.get("sortkey")

        suffix_parts: list[str] = []
        if distkey:
            suffix_parts.append(f"DISTKEY({self.quote_identifier(distkey)})")
        if sortkey:
            if isinstance(sortkey, list):
                cols = ", ".join(self.quote_identifier(c) for c in sortkey)
                suffix_parts.append(f"COMPOUND SORTKEY({cols})")
            else:
                suffix_parts.append(f"SORTKEY({self.quote_identifier(sortkey)})")

        if suffix_parts:
            # Remove trailing semicolon and add hints
            base = base.rstrip(";")
            base += "\n" + "\n".join(suffix_parts) + ";"

        return base
