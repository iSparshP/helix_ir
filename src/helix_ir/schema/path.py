"""Path and PathSegment classes for addressing fields within a schema."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PathSegment:
    """A single segment of a schema path.

    kind='field' with name='foo' represents a struct field named 'foo'.
    kind='array_element' represents descending into an array's element type.
    """

    kind: str  # 'field' or 'array_element'
    name: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("field", "array_element"):
            raise ValueError(f"kind must be 'field' or 'array_element', got {self.kind!r}")
        if self.kind == "field" and not self.name:
            raise ValueError("Field segments must have a name")

    def __str__(self) -> str:
        if self.kind == "array_element":
            return "[]"
        return self.name or ""


@dataclass(frozen=True)
class Path:
    """Immutable dotted path into a nested schema.

    Examples:
        Path.parse("customer.address.city")
        Path.parse("items[].sku")
    """

    segments: tuple[PathSegment, ...]

    def __str__(self) -> str:
        parts: list[str] = []
        for i, seg in enumerate(self.segments):
            if seg.kind == "array_element":
                if parts:
                    parts[-1] = parts[-1] + "[]"
                else:
                    parts.append("[]")
            else:
                parts.append(seg.name or "")
        return ".".join(parts)

    @classmethod
    def parse(cls, path: str) -> "Path":
        """Parse a dotted path string into a Path.

        Supports:
            "customer.address.city"
            "items[].sku"
            "items[].tags[]"
        """
        if not path or path == ".":
            return cls(segments=())

        segments: list[PathSegment] = []
        # Split on dots, but handle [] in names
        parts = path.split(".")
        for part in parts:
            if not part:
                continue
            # Check for array notation
            while "[]" in part:
                name, _, rest = part.partition("[]")
                if name:
                    segments.append(PathSegment(kind="field", name=name))
                segments.append(PathSegment(kind="array_element"))
                part = rest.lstrip(".")
            if part:
                segments.append(PathSegment(kind="field", name=part))

        return cls(segments=tuple(segments))

    def append(self, name: str) -> "Path":
        """Return a new Path with a field segment appended."""
        return Path(segments=self.segments + (PathSegment(kind="field", name=name),))

    def array_element(self) -> "Path":
        """Return a new Path with an array_element segment appended."""
        return Path(segments=self.segments + (PathSegment(kind="array_element"),))

    def is_descendant_of(self, other: "Path") -> bool:
        """Return True if self is a descendant (longer) path of other."""
        if len(self.segments) <= len(other.segments):
            return False
        return self.segments[: len(other.segments)] == other.segments

    def is_root(self) -> bool:
        """Return True if this is the root (empty) path."""
        return len(self.segments) == 0

    def parent(self) -> "Path":
        """Return the parent path (remove last segment)."""
        if not self.segments:
            return self
        return Path(segments=self.segments[:-1])

    def depth(self) -> int:
        """Return the number of segments in this path."""
        return len(self.segments)

    def __truediv__(self, name: str) -> "Path":
        """Syntactic sugar: path / 'field' → path.append('field')."""
        return self.append(name)

    @classmethod
    def root(cls) -> "Path":
        """Return the root (empty) path."""
        return cls(segments=())


# Module-level ROOT constant
PATH_ROOT: Path = Path(segments=())

# Also set as class attribute for Path.ROOT compatibility
Path.ROOT = PATH_ROOT  # type: ignore[attr-defined]
