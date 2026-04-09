"""Schema class — the central immutable data structure of Helix IR."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import pyarrow as pa

from helix_ir.exceptions import PathNotFoundError
from helix_ir.schema.path import Path, PathSegment
from helix_ir.types.core import HelixType


@dataclass(frozen=True)
class Schema:
    """Immutable schema descriptor.

    Fields are stored as a tuple of (name, HelixType) pairs to preserve order
    while remaining hashable.
    """

    name: str
    fields: tuple[tuple[str, HelixType], ...] = field(default_factory=tuple)

    # -------------------------------------------------------------------------
    # Field access
    # -------------------------------------------------------------------------

    def field(self, name: str) -> HelixType:
        """Return the HelixType for a top-level field by name."""
        for fname, ftype in self.fields:
            if fname == name:
                return ftype
        raise PathNotFoundError(f"Field {name!r} not found in schema {self.name!r}")

    def get_field(self, name: str) -> HelixType | None:
        """Return the HelixType for a top-level field, or None if not found."""
        for fname, ftype in self.fields:
            if fname == name:
                return ftype
        return None

    def field_names(self) -> list[str]:
        """Return a list of top-level field names."""
        return [name for name, _ in self.fields]

    def path(self, path: str) -> HelixType:
        """Return the HelixType at the given dotted path.

        Example: schema.path("customer.address.city")
        """
        p = Path.parse(path)
        return self._resolve_path(p)

    def _resolve_path(self, p: Path) -> HelixType:
        """Recursively resolve a Path within this schema."""
        if not p.segments:
            raise PathNotFoundError("Empty path")

        seg = p.segments[0]
        rest = Path(segments=p.segments[1:])

        if seg.kind == "array_element":
            raise PathNotFoundError("Cannot start a path with an array element segment")

        current = self.field(seg.name)  # type: ignore[arg-type]

        # Walk remaining segments
        for seg in rest.segments:
            if seg.kind == "array_element":
                if not pa.types.is_list(current.arrow_type):
                    raise PathNotFoundError(
                        f"Expected list type at {p}, got {current.arrow_type}"
                    )
                current = HelixType(arrow_type=current.arrow_type.value_type)
            else:
                if not pa.types.is_struct(current.arrow_type):
                    raise PathNotFoundError(
                        f"Expected struct type, got {current.arrow_type}"
                    )
                arrow_struct = current.arrow_type
                try:
                    idx = arrow_struct.get_field_index(seg.name)  # type: ignore[attr-defined]
                except Exception:
                    idx = -1
                if idx < 0:
                    raise PathNotFoundError(
                        f"Field {seg.name!r} not found in struct"
                    )
                current = HelixType(arrow_type=arrow_struct.field(idx).type)

        return current

    # -------------------------------------------------------------------------
    # Walking
    # -------------------------------------------------------------------------

    def walk(self) -> Iterable[tuple[Path, HelixType]]:
        """Yield (path, HelixType) for every leaf field in the schema."""
        yield from self._walk_fields(Path.ROOT, self.fields)

    def _walk_fields(
        self, base: Path, fields: tuple[tuple[str, HelixType], ...]
    ) -> Iterable[tuple[Path, HelixType]]:
        for name, ht in fields:
            p = base.append(name)
            yield p, ht
            if pa.types.is_struct(ht.arrow_type):
                nested = _struct_to_fields(ht.arrow_type)
                yield from self._walk_struct(p, nested)
            elif pa.types.is_list(ht.arrow_type):
                elem_type = ht.arrow_type.value_type
                elem_ht = HelixType(arrow_type=elem_type)
                elem_path = p.array_element()
                yield elem_path, elem_ht
                if pa.types.is_struct(elem_type):
                    nested = _struct_to_fields(elem_type)
                    yield from self._walk_struct(elem_path, nested)

    def _walk_struct(
        self, base: Path, fields: list[tuple[str, HelixType]]
    ) -> Iterable[tuple[Path, HelixType]]:
        for name, ht in fields:
            p = base.append(name)
            yield p, ht
            if pa.types.is_struct(ht.arrow_type):
                nested = _struct_to_fields(ht.arrow_type)
                yield from self._walk_struct(p, nested)
            elif pa.types.is_list(ht.arrow_type):
                elem_type = ht.arrow_type.value_type
                elem_ht = HelixType(arrow_type=elem_type)
                elem_path = p.array_element()
                yield elem_path, elem_ht
                if pa.types.is_struct(elem_type):
                    nested = _struct_to_fields(elem_type)
                    yield from self._walk_struct(elem_path, nested)

    def walk_arrays(self) -> Iterable[tuple[Path, HelixType]]:
        """Yield (path, HelixType) for every array field in the schema."""
        for p, ht in self.walk():
            if pa.types.is_list(ht.arrow_type):
                yield p, ht

    # -------------------------------------------------------------------------
    # Arrow interop
    # -------------------------------------------------------------------------

    def to_arrow(self) -> pa.Schema:
        """Convert this schema to a PyArrow Schema."""
        from helix_ir.types.arrow_interop import helix_schema_to_arrow
        return helix_schema_to_arrow(list(self.fields))

    @classmethod
    def from_arrow(cls, name: str, schema: pa.Schema) -> "Schema":
        """Create a Schema from a PyArrow Schema."""
        from helix_ir.types.arrow_interop import arrow_schema_to_helix
        pairs = arrow_schema_to_helix(schema)
        return cls(name=name, fields=tuple(pairs))

    # -------------------------------------------------------------------------
    # JSON serialization
    # -------------------------------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        """Serialize this schema to a JSON-compatible dict."""
        from helix_ir.schema.serialization import helix_type_to_json
        return {
            "name": self.name,
            "fields": [
                {"name": fname, "type": helix_type_to_json(ftype)}
                for fname, ftype in self.fields
            ],
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Schema":
        """Deserialize a Schema from a JSON-compatible dict."""
        from helix_ir.schema.serialization import helix_type_from_json
        fields = tuple(
            (f["name"], helix_type_from_json(f["type"])) for f in data["fields"]
        )
        return cls(name=data["name"], fields=fields)

    # -------------------------------------------------------------------------
    # Mutation (returns new Schema)
    # -------------------------------------------------------------------------

    def add_field(self, name: str, ftype: HelixType) -> "Schema":
        """Return a new Schema with the given field added (or replaced)."""
        new_fields = tuple(
            (n, t) for n, t in self.fields if n != name
        ) + ((name, ftype),)
        return Schema(name=self.name, fields=new_fields)

    def drop_field(self, name: str) -> "Schema":
        """Return a new Schema with the given field removed."""
        return Schema(
            name=self.name,
            fields=tuple((n, t) for n, t in self.fields if n != name),
        )

    def rename(self, new_name: str) -> "Schema":
        """Return a new Schema with a different name."""
        return Schema(name=new_name, fields=self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __contains__(self, name: str) -> bool:
        return any(n == name for n, _ in self.fields)

    def __repr__(self) -> str:
        field_strs = ", ".join(f"{n}: {t.arrow_type}" for n, t in self.fields)
        return f"Schema({self.name!r}, [{field_strs}])"


def _struct_to_fields(t: pa.StructType) -> list[tuple[str, HelixType]]:
    """Convert a pa.StructType to a list of (name, HelixType) pairs."""
    return [
        (t.field(i).name, HelixType(arrow_type=t.field(i).type))
        for i in range(t.num_fields)
    ]
