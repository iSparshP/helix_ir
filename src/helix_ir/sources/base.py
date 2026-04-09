"""Source protocol — all data sources implement this interface."""

from __future__ import annotations

from typing import Any, Iterable, Protocol, runtime_checkable


@runtime_checkable
class Source(Protocol):
    """Protocol for all Helix IR data sources."""

    def read(self) -> Iterable[dict[str, Any]]:
        """Yield documents from this source."""
        ...

    def schema_hint(self) -> dict[str, Any] | None:
        """Return an optional schema hint dict, or None."""
        ...
