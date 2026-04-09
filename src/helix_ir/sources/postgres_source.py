"""PostgreSQL source (requires psycopg)."""

from __future__ import annotations

from typing import Any, Iterable

try:
    import psycopg  # type: ignore[import]
    _PSYCOPG_AVAILABLE = True
except ImportError:
    _PSYCOPG_AVAILABLE = False


class PostgresSource:
    """Read documents from a PostgreSQL table or query."""

    def __init__(
        self,
        connection_string: str,
        query: str | None = None,
        table: str | None = None,
        batch_size: int = 1000,
    ) -> None:
        if not _PSYCOPG_AVAILABLE:
            raise ImportError(
                "psycopg is required for PostgresSource. "
                "Install it with: pip install 'helix-ir[postgres]'"
            )
        if not query and not table:
            raise ValueError("Either 'query' or 'table' must be provided.")
        self.connection_string = connection_string
        self._query = query or f"SELECT * FROM {table}"
        self.batch_size = batch_size

    def read(self) -> Iterable[dict[str, Any]]:
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(self._query)
                while True:
                    rows = cur.fetchmany(self.batch_size)
                    if not rows:
                        break
                    yield from rows

    def schema_hint(self) -> dict[str, Any] | None:
        return None
