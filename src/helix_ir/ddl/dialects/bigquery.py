"""Google BigQuery DDL dialect."""

from __future__ import annotations

import pyarrow as pa

from helix_ir.ddl.dialects.base import BaseDialect, DDLOptions
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType


class BigQueryDialect(BaseDialect):
    """Google BigQuery DDL dialect."""

    DIALECT_NAME = "bigquery"

    TYPE_MAP: dict[str, str] = {
        "null": "STRING",
        "bool": "BOOL",
        "int8": "INT64",
        "int16": "INT64",
        "int32": "INT64",
        "int64": "INT64",
        "uint8": "INT64",
        "uint16": "INT64",
        "uint32": "INT64",
        "uint64": "INT64",
        "float16": "FLOAT64",
        "float32": "FLOAT64",
        "float64": "FLOAT64",
        "string": "STRING",
        "large_string": "STRING",
        "binary": "BYTES",
        "date32": "DATE",
        "date64": "DATE",
    }

    def _timestamp_type(self, t: pa.TimestampType) -> str:
        return "TIMESTAMP"

    def _json_type(self) -> str:
        return "JSON"

    def _list_type(self, t: pa.ListType) -> str:
        inner = self.arrow_to_sql_type(t.value_type)
        return f"ARRAY<{inner}>"

    def _struct_type(self, t: pa.StructType) -> str:
        if t.num_fields == 0:
            return "STRUCT<>"
        fields = ", ".join(
            f"{t.field(i).name} {self.arrow_to_sql_type(t.field(i).type)}"
            for i in range(t.num_fields)
        )
        return f"STRUCT<{fields}>"

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def _column_def(self, name: str, ht: HelixType) -> str:
        sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
        mode = "NULLABLE" if ht.null_ratio > 0 else "NOT NULL"
        col = f"  {self.quote_identifier(name)} {sql_type} OPTIONS(description='')"
        return col

    def compile_create_table(
        self,
        schema: Schema,
        options: DDLOptions,
    ) -> str:
        """Generate BigQuery CREATE TABLE with partitioning/clustering hints."""
        table_name = self._table_name(schema.name, options)
        column_defs: list[str] = []

        for fname, ht in schema.fields:
            sql_type = self.arrow_to_sql_type(ht.arrow_type, ht)
            desc = ht.description or ""
            col = f"  {self.quote_identifier(fname)} {sql_type} OPTIONS(description='{desc}')"
            column_defs.append(col)

        exists_clause = "IF NOT EXISTS " if options.if_not_exists else ""
        body = ",\n".join(column_defs)
        stmt = f"CREATE TABLE {exists_clause}{table_name} (\n{body}\n)"

        # Partitioning
        partition_col = options.extra_options.get("partition_by")
        if partition_col:
            stmt += f"\nPARTITION BY DATE({self.quote_identifier(partition_col)})"

        # Clustering
        cluster_cols = options.extra_options.get("cluster_by")
        if cluster_cols:
            if isinstance(cluster_cols, list):
                cols = ", ".join(self.quote_identifier(c) for c in cluster_cols)
            else:
                cols = self.quote_identifier(cluster_cols)
            stmt += f"\nCLUSTER BY {cols}"

        stmt += ";"
        return stmt
