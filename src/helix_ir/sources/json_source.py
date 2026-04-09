"""JSON file source."""

from __future__ import annotations

import json
import os
from typing import Any, Iterable


class JSONSource:
    """Read documents from a JSON or NDJSON file."""

    def __init__(self, path: str, format: str = "auto") -> None:
        """
        Args:
            path: Path to a JSON file (array of objects) or NDJSON file.
            format: 'json', 'ndjson', or 'auto' (detect by extension).
        """
        self.path = path
        self._format = format

    def _detect_format(self) -> str:
        ext = os.path.splitext(self.path)[1].lower()
        if ext in (".ndjson", ".jsonl"):
            return "ndjson"
        return "json"

    def read(self) -> Iterable[dict[str, Any]]:
        fmt = self._format if self._format != "auto" else self._detect_format()
        with open(self.path, encoding="utf-8") as f:
            if fmt == "ndjson":
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
            else:
                data = json.load(f)
                if isinstance(data, list):
                    yield from data
                elif isinstance(data, dict):
                    yield data

    def schema_hint(self) -> dict[str, Any] | None:
        return None
