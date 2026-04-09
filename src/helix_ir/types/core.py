"""Core HelixType dataclass."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa


@dataclass(frozen=True)
class HelixType:
    """Immutable type descriptor for a single field in a Helix schema.

    Carries both the Arrow physical type and rich metadata such as
    null ratio, cardinality estimate, semantic tag, and PII class.
    """

    arrow_type: pa.DataType
    null_ratio: float = 0.0
    cardinality_estimate: int | None = None
    sample_count: int = 0
    confidence: float = 1.0
    semantic: str | None = None
    pii_class: str | None = None
    source_path: str | None = None
    min_value: Any = None
    max_value: Any = None
    description: str | None = None
    tags: frozenset[str] = field(default_factory=frozenset)

    def evolve(self, **changes: Any) -> "HelixType":
        """Return a new HelixType with the given fields changed."""
        return dataclasses.replace(self, **changes)

    def is_nullable(self) -> bool:
        """Return True if this field ever observed nulls."""
        return self.null_ratio > 0.0

    def is_list(self) -> bool:
        """Return True if the Arrow type is a list type."""
        return pa.types.is_list(self.arrow_type) or pa.types.is_large_list(self.arrow_type)

    def is_struct(self) -> bool:
        """Return True if the Arrow type is a struct type."""
        return pa.types.is_struct(self.arrow_type)

    def is_null(self) -> bool:
        """Return True if the Arrow type is null (all-null column)."""
        return pa.types.is_null(self.arrow_type)

    def __repr__(self) -> str:
        parts = [f"arrow_type={self.arrow_type}"]
        if self.null_ratio > 0:
            parts.append(f"null_ratio={self.null_ratio:.3f}")
        if self.semantic:
            parts.append(f"semantic={self.semantic!r}")
        if self.pii_class:
            parts.append(f"pii_class={self.pii_class!r}")
        return f"HelixType({', '.join(parts)})"
