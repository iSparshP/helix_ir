"""Databricks SQL DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect, DDLOptions
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


class DatabricksDialect(BaseDialect):
    """Databricks SQL DDL dialect (Delta Lake)."""

    DIALECT_NAME = "databricks"

    TYPE_MAP: dict[str, str] = {
        "null": "STRING",
        "bool": "BOOLEAN",
        "int8": "TINYINT",
        "int16": "SMALLINT",
        "int32": "INT",
        "int64": "BIGINT",
        "uint8": "TINYINT",
        "uint16": "SMALLINT",
        "uint32": "INT",
        "uint64": "BIGINT",
        "float16": "FLOAT",
        "float32": "FLOAT",
        "float64": "DOUBLE",
        "string": "STRING",
        "large_string": "STRING",
        "binary": "BINARY",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "STRING"  # Store as JSON string in Databricks

    def _list_type(self, t: pa.ListType) -> str:
        inner = self.arrow_to_sql_type(t.value_type)
        return f"ARRAY<{inner}>"

    def _struct_type(self, t: pa.StructType) -> str:
        if t.num_fields == 0:
            return "STRUCT<>"
        fields = ", ".join(
            f"{t.field(i).name}: {self.arrow_to_sql_type(t.field(i).type)}"
            for i in range(t.num_fields)
        )
        return f"STRUCT<{fields}>"

    def compile_create_table(
        self,
        schema: Schema,
        options: DDLOptions,
    ) -> str:
        """Generate a Databricks Delta Lake CREATE TABLE statement."""
        table_name = self._table_name(schema.name, options)
        column_defs: list[str] = []

        for fname, ht in schema.fields:
            sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
            nullable = "" if ht.null_ratio > 0 else " NOT NULL"
            comment = f" COMMENT '{ht.description}'" if ht.description else ""
            col = f"  {self.quote_identifier(fname)} {sql_type}{nullable}{comment}"
            column_defs.append(col)

        exists_clause = "IF NOT EXISTS " if options.if_not_exists else ""
        body = ",\n".join(column_defs)
        stmt = f"CREATE TABLE {exists_clause}{table_name} (\n{body}\n)\nUSING DELTA"

        # Partitioning
        partition_cols = options.extra_options.get("partition_by")
        if partition_cols:
            if isinstance(partition_cols, list):
                cols = ", ".join(self.quote_identifier(c) for c in partition_cols)
            else:
                cols = self.quote_identifier(partition_cols)
            stmt += f"\nPARTITIONED BY ({cols})"

        stmt += ";"
        return stmt
