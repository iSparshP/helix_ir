"""NormalizationPlan — output of the normalize() function."""

from __future__ import annotations

from dataclasses import dataclass, field

from helix_ir.lineage.graph import Lineage
from helix_ir.schema.schema import Schema


@dataclass
class ForeignKey:
    """A foreign key relationship between two tables."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    def __repr__(self) -> str:
        return (
            f"ForeignKey({self.from_table}.{self.from_column} "
            f"→ {self.to_table}.{self.to_column})"
        )


@dataclass
class NormalizationPlan:
    """The output of normalize(): a set of tables with FK relationships."""

    tables: list[Schema] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    lineage: Lineage = field(default_factory=Lineage)

    def root_table(self) -> Schema:
        """Return the root (first) table — typically the main entity table."""
        if not self.tables:
            raise ValueError("NormalizationPlan has no tables")
        return self.tables[0]

    def table_names(self) -> list[str]:
        return [t.name for t in self.tables]

    def get_table(self, name: str) -> Schema | None:
        for t in self.tables:
            if t.name == name:
                return t
        return None

    def __repr__(self) -> str:
        names = ", ".join(t.name for t in self.tables)
        return f"NormalizationPlan(tables=[{names}], fks={len(self.foreign_keys)})"
