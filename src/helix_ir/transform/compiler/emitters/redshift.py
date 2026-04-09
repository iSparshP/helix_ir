"""Amazon Redshift SQL emitter."""

from __future__ import annotations

from helix_ir.transform.compiler.emitters.base import BaseEmitter


class RedshiftEmitter(BaseEmitter):
    """Amazon Redshift SQL emitter."""

    DIALECT = "redshift"
