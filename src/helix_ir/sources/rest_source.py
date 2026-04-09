"""REST API source."""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from typing import Any, Iterable


class RestSource:
    """Read documents from a paginated REST API."""

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        data_key: str | None = None,
        pagination: dict[str, Any] | None = None,
        max_pages: int = 100,
    ) -> None:
        """
        Args:
            url: Base URL of the API endpoint.
            headers: Optional HTTP headers (e.g. Authorization).
            data_key: JSON key containing the array of records (e.g. 'data', 'results').
            pagination: Dict with pagination config:
                {'type': 'page', 'page_param': 'page', 'size_param': 'per_page', 'page_size': 100}
                {'type': 'cursor', 'cursor_key': 'next_cursor', 'cursor_param': 'cursor'}
            max_pages: Maximum number of pages to fetch.
        """
        self.url = url
        self.headers = headers or {}
        self.data_key = data_key
        self.pagination = pagination or {}
        self.max_pages = max_pages

    def read(self) -> Iterable[dict[str, Any]]:
        page = 1
        cursor = None

        for _ in range(self.max_pages):
            url = self._build_url(page, cursor)
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode())

            records = self._extract_records(data)
            if not records:
                break

            yield from records

            # Handle pagination
            page_type = self.pagination.get("type", "none")
            if page_type == "page":
                page += 1
            elif page_type == "cursor":
                cursor_key = self.pagination.get("cursor_key", "next_cursor")
                cursor = data.get(cursor_key)
                if not cursor:
                    break
            else:
                break  # No pagination

    def _build_url(self, page: int, cursor: str | None) -> str:
        page_type = self.pagination.get("type", "none")
        params: dict[str, str] = {}

        if page_type == "page":
            params[self.pagination.get("page_param", "page")] = str(page)
            if "size_param" in self.pagination:
                params[self.pagination["size_param"]] = str(
                    self.pagination.get("page_size", 100)
                )
        elif page_type == "cursor" and cursor:
            params[self.pagination.get("cursor_param", "cursor")] = cursor

        if params:
            sep = "&" if "?" in self.url else "?"
            return self.url + sep + urllib.parse.urlencode(params)
        return self.url

    def _extract_records(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if self.data_key and self.data_key in data:
                return data[self.data_key]
            # Try common keys
            for key in ("data", "results", "items", "records"):
                if key in data:
                    return data[key]
        return []

    def schema_hint(self) -> dict[str, Any] | None:
        return None
