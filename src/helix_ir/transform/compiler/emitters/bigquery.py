"""Google BigQuery SQL emitter."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter


class BigQueryEmitter(BaseEmitter):
    """Google BigQuery SQL emitter."""

    DIALECT = "bigquery"

    def quote(self, name: str) -> str:
        return f"`{name}`"
