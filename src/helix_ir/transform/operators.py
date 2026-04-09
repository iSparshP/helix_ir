"""GroupedTable and WindowTable operators."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix_ir.transform.table import Table
    from helix_ir.transform.expression import Expression


class GroupedTable:
    """A table after a group_by() call. Call .agg() to produce a new Table."""

    def __init__(self, table: "Table", group_by: list["Expression"]) -> None:
        self._table = table
        self._group_by = group_by

    def agg(self, *exprs: "Expression") -> "Table":
        """Apply aggregate expressions."""
        from helix_ir.transform.compiler.logical import Aggregate
        from helix_ir.transform.table import Table

        plan = Aggregate(
            input=self._table._plan,
            group_by=self._group_by,
            agg=list(exprs),
        )
        return Table._from_plan(plan)

    def __repr__(self) -> str:
        cols = ", ".join(str(e) for e in self._group_by)
        return f"GroupedTable(by=[{cols}])"


class WindowTable:
    """A window frame for window function calculations."""

    def __init__(
        self,
        table: "Table",
        partition_by: list["Expression"],
        order_by: list["Expression"],
    ) -> None:
        self._table = table
        self._partition_by = partition_by
        self._order_by = order_by

    def over(self, *exprs: "Expression") -> "Table":
        """Apply window expressions (simplified: returns the original table for now)."""
        return self._table
