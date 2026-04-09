"""Data lineage graph: track field-level transformations."""

from __future__ import annotations

from dataclasses import dataclass, field

from helix_ir.schema.path import Path


@dataclass(frozen=True)
class LineageEdge:
    """A directed edge in the lineage graph."""

    source: Path
    target: Path
    transform: str | None = None
    confidence: float = 1.0


class Lineage:
    """A lineage graph tracking field-level data flow."""

    def __init__(self) -> None:
        self._edges: list[LineageEdge] = []

    def record(
        self,
        source: Path | str,
        target: Path | str,
        transform: str | None = None,
        confidence: float = 1.0,
    ) -> None:
        """Record a lineage edge from source to target."""
        if isinstance(source, str):
            source = Path.parse(source)
        if isinstance(target, str):
            target = Path.parse(target)
        self._edges.append(LineageEdge(
            source=source,
            target=target,
            transform=transform,
            confidence=confidence,
        ))

    def upstream(self, path: Path | str) -> list[LineageEdge]:
        """Return all edges where target == path."""
        if isinstance(path, str):
            path = Path.parse(path)
        return [e for e in self._edges if e.target == path]

    def downstream(self, path: Path | str) -> list[LineageEdge]:
        """Return all edges where source == path."""
        if isinstance(path, str):
            path = Path.parse(path)
        return [e for e in self._edges if e.source == path]

    def all_edges(self) -> list[LineageEdge]:
        """Return all recorded edges."""
        return list(self._edges)

    def to_openlineage(self) -> list[dict]:
        """Export lineage as OpenLineage-compatible dicts."""
        result = []
        for edge in self._edges:
            result.append(
                {
                    "eventType": "COMPLETE",
                    "inputs": [
                        {
                            "namespace": "helix",
                            "name": str(edge.source),
                            "facets": {},
                        }
                    ],
                    "outputs": [
                        {
                            "namespace": "helix",
                            "name": str(edge.target),
                            "facets": {},
                        }
                    ],
                    "job": {
                        "namespace": "helix",
                        "name": edge.transform or "identity",
                    },
                    "run": {"runId": "00000000-0000-0000-0000-000000000000"},
                    "producer": "helix_ir",
                    "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
                }
            )
        return result

    def to_dot(self) -> str:
        """Export lineage as a Graphviz DOT string."""
        lines = ["digraph lineage {"]
        lines.append('  rankdir="LR";')
        seen_nodes: set[str] = set()

        for edge in self._edges:
            src_str = str(edge.source)
            tgt_str = str(edge.target)

            for node in (src_str, tgt_str):
                if node not in seen_nodes:
                    escaped = node.replace('"', '\\"')
                    lines.append(f'  "{escaped}";')
                    seen_nodes.add(node)

            src_escaped = src_str.replace('"', '\\"')
            tgt_escaped = tgt_str.replace('"', '\\"')
            label = f' [label="{edge.transform or ""}"]' if edge.transform else ""
            lines.append(f'  "{src_escaped}" -> "{tgt_escaped}"{label};')

        lines.append("}")
        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self._edges)

    def __repr__(self) -> str:
        return f"Lineage({len(self._edges)} edges)"
