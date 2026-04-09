"""JSON serialization/deserialization for Schema and HelixType."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from helix_ir.types.core import HelixType


def helix_type_to_json(ht: HelixType) -> dict[str, Any]:
    """Serialize a HelixType to a JSON-compatible dict."""
    return {
        "arrow_type": _arrow_type_to_str(ht.arrow_type),
        "null_ratio": ht.null_ratio,
        "cardinality_estimate": ht.cardinality_estimate,
        "sample_count": ht.sample_count,
        "confidence": ht.confidence,
        "semantic": ht.semantic,
        "pii_class": ht.pii_class,
        "source_path": ht.source_path,
        "min_value": _serialize_value(ht.min_value),
        "max_value": _serialize_value(ht.max_value),
        "description": ht.description,
        "tags": list(ht.tags),
    }


def helix_type_from_json(data: dict[str, Any]) -> HelixType:
    """Deserialize a HelixType from a JSON-compatible dict."""
    return HelixType(
        arrow_type=_str_to_arrow_type(data["arrow_type"]),
        null_ratio=data.get("null_ratio", 0.0),
        cardinality_estimate=data.get("cardinality_estimate"),
        sample_count=data.get("sample_count", 0),
        confidence=data.get("confidence", 1.0),
        semantic=data.get("semantic"),
        pii_class=data.get("pii_class"),
        source_path=data.get("source_path"),
        min_value=data.get("min_value"),
        max_value=data.get("max_value"),
        description=data.get("description"),
        tags=frozenset(data.get("tags", [])),
    )


def _serialize_value(v: Any) -> Any:
    """Convert a Python value to JSON-serializable form."""
    if v is None:
        return None
    import datetime
    from decimal import Decimal
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


def _arrow_type_to_str(t: pa.DataType) -> str:  # noqa: C901
    """Serialize an Arrow DataType to a string."""
    if pa.types.is_null(t):
        return "null"
    if pa.types.is_boolean(t):
        return "bool"
    if t == pa.int8():
        return "int8"
    if t == pa.int16():
        return "int16"
    if t == pa.int32():
        return "int32"
    if t == pa.int64():
        return "int64"
    if t == pa.uint8():
        return "uint8"
    if t == pa.uint16():
        return "uint16"
    if t == pa.uint32():
        return "uint32"
    if t == pa.uint64():
        return "uint64"
    if t == pa.float16():
        return "float16"
    if t == pa.float32():
        return "float32"
    if t == pa.float64():
        return "float64"
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_large_string(t):
        return "large_string"
    if pa.types.is_binary(t):
        return "binary"
    if t == pa.date32():
        return "date32"
    if t == pa.date64():
        return "date64"
    if pa.types.is_timestamp(t):
        tz = f",{t.tz}" if t.tz else ""
        return f"timestamp[{t.unit}{tz}]"
    if pa.types.is_duration(t):
        return f"duration[{t.unit}]"
    if pa.types.is_list(t):
        return f"list<{_arrow_type_to_str(t.value_type)}>"
    if pa.types.is_struct(t):
        fields = ",".join(
            f"{t.field(i).name}:{_arrow_type_to_str(t.field(i).type)}"
            for i in range(t.num_fields)
        )
        return f"struct<{fields}>"
    if pa.types.is_decimal(t):
        return f"decimal128({t.precision},{t.scale})"
    return str(t)


def _str_to_arrow_type(s: str) -> pa.DataType:  # noqa: C901
    """Deserialize an Arrow DataType from a string."""
    simple = {
        "null": pa.null(),
        "bool": pa.bool_(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float16": pa.float16(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "string": pa.string(),
        "utf8": pa.string(),
        "large_string": pa.large_string(),
        "binary": pa.binary(),
        "date32": pa.date32(),
        "date64": pa.date64(),
    }
    if s in simple:
        return simple[s]

    if s.startswith("timestamp["):
        inner = s[10:-1]
        if "," in inner:
            unit, tz = inner.split(",", 1)
        else:
            unit, tz = inner, None
        return pa.timestamp(unit, tz=tz or None)

    if s.startswith("duration["):
        unit = s[9:-1]
        return pa.duration(unit)

    if s.startswith("decimal128("):
        inner = s[11:-1]
        precision, scale = inner.split(",")
        return pa.decimal128(int(precision), int(scale))

    if s.startswith("list<"):
        inner = s[5:-1]
        return pa.list_(_str_to_arrow_type(inner))

    if s.startswith("struct<"):
        inner = s[7:-1]
        if not inner:
            return pa.struct([])
        fields = _split_struct_fields(inner)
        arrow_fields: list[pa.Field] = []
        for f in fields:
            name, _, type_str = f.partition(":")
            arrow_fields.append(pa.field(name, _str_to_arrow_type(type_str)))
        return pa.struct(arrow_fields)

    # Fallback: use string
    return pa.string()


def _split_struct_fields(s: str) -> list[str]:
    """Split struct field definitions respecting nested angle brackets."""
    fields: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in s:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            fields.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        fields.append("".join(current).strip())
    return fields
