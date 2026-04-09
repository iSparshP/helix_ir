"""Snowflake SQL emitter."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter


class SnowflakeEmitter(BaseEmitter):
    """Snowflake SQL emitter."""

    DIALECT = "snowflake"
