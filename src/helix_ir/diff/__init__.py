"""helix_ir.diff — schema diffing and change classification."""

from helix_ir.diff.classifier import SchemaDiff, SchemaChange, diff

__all__ = ["diff", "SchemaDiff", "SchemaChange"]
