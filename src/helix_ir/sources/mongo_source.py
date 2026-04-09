"""MongoDB source (requires pymongo)."""

from __future__ import annotations

from typing import Any, Iterable

try:
    import pymongo  # type: ignore[import]
    _PYMONGO_AVAILABLE = True
except ImportError:
    _PYMONGO_AVAILABLE = False


class MongoSource:
    """Read documents from a MongoDB collection."""

    def __init__(
        self,
        connection_string: str,
        database: str,
        collection: str,
        query: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int = 0,
    ) -> None:
        if not _PYMONGO_AVAILABLE:
            raise ImportError(
                "pymongo is required for MongoSource. "
                "Install it with: pip install 'helix-ir[mongo]'"
            )
        self.connection_string = connection_string
        self.database = database
        self.collection = collection
        self.query = query or {}
        self.projection = projection
        self.limit = limit

    def read(self) -> Iterable[dict[str, Any]]:
        client = pymongo.MongoClient(self.connection_string)
        try:
            coll = client[self.database][self.collection]
            cursor = coll.find(self.query, self.projection)
            if self.limit:
                cursor = cursor.limit(self.limit)
            for doc in cursor:
                doc.pop("_id", None)  # Remove MongoDB ObjectId
                yield doc
        finally:
            client.close()

    def schema_hint(self) -> dict[str, Any] | None:
        return None
