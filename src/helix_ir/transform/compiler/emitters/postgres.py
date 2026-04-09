"""PostgreSQL SQL emitter."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter
from helix_ir.transform.compiler.logical import Filter, Scan


class PostgresEmitter(BaseEmitter):
    """PostgreSQL SQL emitter."""

    DIALECT = "postgres"

    def emit_filter(self, plan: Filter) -> str:
        inner = self.emit(plan.input)
        predicate = self.emit_expression(plan.predicate)
        return f"SELECT * FROM {inner} WHERE {predicate}"
