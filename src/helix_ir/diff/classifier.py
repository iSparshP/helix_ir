"""Schema diff: compare two schemas and classify changes."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from helix_ir.schema.path import Path
from helix_ir.schema.schema import Schema
from helix_ir.types.core import HelixType
from helix_ir.types.lattice import subsumes


@dataclass(frozen=True)
class SchemaChange:
    """A single change between two schema versions."""

    path: Path
    kind: str  # 'added', 'removed', 'type_changed', 'nullable_changed',
    #            'semantic_changed', 'pii_changed', 'description_changed'
    severity: str  # 'safe', 'risky', 'breaking'
    old_type: HelixType | None
    new_type: HelixType | None
    description: str


@dataclass(frozen=True)
class SchemaDiff:
    """Result of comparing two schemas."""

    old_name: str
    new_name: str
    changes: tuple[SchemaChange, ...]

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.severity == "breaking" for c in self.changes)

    @property
    def has_risky_changes(self) -> bool:
        return any(c.severity in ("breaking", "risky") for c in self.changes)

    def filter(self, severity: str) -> "SchemaDiff":
        """Return a new SchemaDiff containing only changes of the given severity."""
        filtered = tuple(c for c in self.changes if c.severity == severity)
        return SchemaDiff(
            old_name=self.old_name,
            new_name=self.new_name,
            changes=filtered,
        )

    def filter_kind(self, kind: str) -> "SchemaDiff":
        """Return a new SchemaDiff containing only changes of the given kind."""
        filtered = tuple(c for c in self.changes if c.kind == kind)
        return SchemaDiff(
            old_name=self.old_name,
            new_name=self.new_name,
            changes=filtered,
        )

    def summary(self) -> dict[str, int]:
        """Return a summary count of changes by severity."""
        counts: dict[str, int] = {"safe": 0, "risky": 0, "breaking": 0}
        for c in self.changes:
            counts[c.severity] = counts.get(c.severity, 0) + 1
        return counts

    def __len__(self) -> int:
        return len(self.changes)

    def __bool__(self) -> bool:
        return len(self.changes) > 0


def diff(old: Schema, new: Schema) -> SchemaDiff:
    """Compare two schemas and return a SchemaDiff.

    Classification rules:
    - Field removed: BREAKING (downstream consumers break)
    - Field added (nullable): SAFE
    - Field added (non-nullable): RISKY (existing data won't have this field)
    - Type narrowed (e.g. string → int): BREAKING
    - Type widened (e.g. int32 → int64): SAFE
    - Type changed incompatibly: BREAKING
    - Nullable → non-nullable: RISKY
    - Non-nullable → nullable: SAFE
    - Semantic changed: RISKY
    - PII class added: RISKY
    - PII class removed: SAFE (becoming less sensitive)
    """
    changes: list[SchemaChange] = []

    old_fields = dict(old.fields)
    new_fields = dict(new.fields)

    all_names = list(old_fields.keys())
    for n in new_fields:
        if n not in old_fields:
            all_names.append(n)

    for name in all_names:
        path = Path.parse(name)
        old_ht = old_fields.get(name)
        new_ht = new_fields.get(name)

        if old_ht is None and new_ht is not None:
            # Field added
            is_nullable = new_ht.null_ratio > 0 or new_ht.is_nullable()
            severity = "safe" if is_nullable else "risky"
            changes.append(
                SchemaChange(
                    path=path,
                    kind="added",
                    severity=severity,
                    old_type=None,
                    new_type=new_ht,
                    description=f"Field '{name}' added ({'nullable' if is_nullable else 'non-nullable'})",
                )
            )

        elif old_ht is not None and new_ht is None:
            # Field removed — always breaking
            changes.append(
                SchemaChange(
                    path=path,
                    kind="removed",
                    severity="breaking",
                    old_type=old_ht,
                    new_type=None,
                    description=f"Field '{name}' removed",
                )
            )

        else:
            assert old_ht is not None and new_ht is not None
            # Compare the types
            type_changes = _classify_type_change(path, name, old_ht, new_ht)
            changes.extend(type_changes)

    return SchemaDiff(
        old_name=old.name,
        new_name=new.name,
        changes=tuple(changes),
    )


def _classify_type_change(  # noqa: C901
    path: Path,
    name: str,
    old_ht: HelixType,
    new_ht: HelixType,
) -> list[SchemaChange]:
    """Classify differences between two HelixType instances for the same field."""
    changes: list[SchemaChange] = []

    # Check Arrow type change
    if old_ht.arrow_type != new_ht.arrow_type:
        severity = _classify_arrow_type_change(old_ht.arrow_type, new_ht.arrow_type)
        desc = (
            f"Field '{name}' type changed from {old_ht.arrow_type} to {new_ht.arrow_type}"
        )
        changes.append(
            SchemaChange(
                path=path,
                kind="type_changed",
                severity=severity,
                old_type=old_ht,
                new_type=new_ht,
                description=desc,
            )
        )

    # Check nullability change
    old_nullable = old_ht.null_ratio > 0
    new_nullable = new_ht.null_ratio > 0
    if old_nullable != new_nullable:
        if old_nullable and not new_nullable:
            # Became non-nullable: RISKY (existing data may have nulls)
            severity = "risky"
            desc = f"Field '{name}' became non-nullable"
        else:
            # Became nullable: SAFE
            severity = "safe"
            desc = f"Field '{name}' became nullable"
        changes.append(
            SchemaChange(
                path=path,
                kind="nullable_changed",
                severity=severity,
                old_type=old_ht,
                new_type=new_ht,
                description=desc,
            )
        )

    # Check semantic change
    if old_ht.semantic != new_ht.semantic:
        changes.append(
            SchemaChange(
                path=path,
                kind="semantic_changed",
                severity="risky",
                old_type=old_ht,
                new_type=new_ht,
                description=(
                    f"Field '{name}' semantic changed from {old_ht.semantic!r} "
                    f"to {new_ht.semantic!r}"
                ),
            )
        )

    # Check PII change
    if old_ht.pii_class != new_ht.pii_class:
        if old_ht.pii_class is None and new_ht.pii_class is not None:
            severity = "risky"
            desc = f"Field '{name}' now classified as PII ({new_ht.pii_class!r})"
        elif old_ht.pii_class is not None and new_ht.pii_class is None:
            severity = "safe"
            desc = f"Field '{name}' PII classification removed (was {old_ht.pii_class!r})"
        else:
            severity = "risky"
            desc = (
                f"Field '{name}' PII class changed from {old_ht.pii_class!r} "
                f"to {new_ht.pii_class!r}"
            )
        changes.append(
            SchemaChange(
                path=path,
                kind="pii_changed",
                severity=severity,
                old_type=old_ht,
                new_type=new_ht,
                description=desc,
            )
        )

    return changes


def _classify_arrow_type_change(
    old_type: pa.DataType,
    new_type: pa.DataType,
) -> str:
    """Classify the severity of changing from old_type to new_type."""
    from helix_ir.types.core import HelixType

    old_ht = HelixType(arrow_type=old_type)
    new_ht = HelixType(arrow_type=new_type)

    # New type subsumes old → safe widening
    if subsumes(new_ht, old_ht):
        return "safe"

    # Old type subsumes new but new doesn't subsume old → narrowing is risky
    if subsumes(old_ht, new_ht):
        return "risky"

    # String to non-string: risky (may fail parsing)
    if pa.types.is_string(old_type) and not pa.types.is_string(new_type):
        return "risky"

    # Incompatible types: breaking
    return "breaking"
