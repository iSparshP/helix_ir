"""DuckDB SQL emitter (reference implementation)."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter
from helix_ir.transform.compiler.logical import Limit


class DuckDBEmitter(BaseEmitter):
    """DuckDB SQL emitter — reference implementation."""

    DIALECT = "duckdb"

    def emit_limit(self, plan: Limit) -> str:
        inner = self.emit(plan.input)
        stmt = f"SELECT * FROM ({inner}) AS _limited\nLIMIT {plan.n}"
        if plan.offset:
            stmt += f" OFFSET {plan.offset}"
        return stmt
