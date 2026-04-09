"""Parquet file source."""

from __future__ import annotations

from typing import Any, Iterable


class ParquetSource:
    """Read documents from a Parquet file using PyArrow."""

    def __init__(self, path: str, batch_size: int = 1000) -> None:
        self.path = path
        self.batch_size = batch_size

    def read(self) -> Iterable[dict[str, Any]]:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(self.path)
        for batch in pf.iter_batches(batch_size=self.batch_size):
            tbl = batch.to_pydict()
            n_rows = len(next(iter(tbl.values()), []))
            for i in range(n_rows):
                yield {k: v[i] for k, v in tbl.items()}

    def schema_hint(self) -> dict[str, Any] | None:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(self.path)
        schema = pf.schema_arrow
        return {"arrow_schema": schema}
