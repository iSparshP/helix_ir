"""Databricks SQL emitter."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter


class DatabricksEmitter(BaseEmitter):
    """Databricks SQL emitter."""

    DIALECT = "databricks"
