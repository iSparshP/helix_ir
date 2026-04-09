"""Type lattice: join, meet, and subsumes operations for HelixType."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from helix_ir.types.core import HelixType

# Numeric widening order (lower index = narrower)
_INT_WIDENING: list[pa.DataType] = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
]

_UINT_WIDENING: list[pa.DataType] = [
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
]

_FLOAT_WIDENING: list[pa.DataType] = [
    pa.float16(),
    pa.float32(),
    pa.float64(),
]


def _int_rank(t: pa.DataType) -> int | None:
    """Return widening rank for integer types, or None."""
    for i, candidate in enumerate(_INT_WIDENING):
        if t == candidate:
            return i
    return None


def _uint_rank(t: pa.DataType) -> int | None:
    for i, candidate in enumerate(_UINT_WIDENING):
        if t == candidate:
            return i
    return None


def _float_rank(t: pa.DataType) -> int | None:
    for i, candidate in enumerate(_FLOAT_WIDENING):
        if t == candidate:
            return i
    return None


def _is_integer(t: pa.DataType) -> bool:
    return pa.types.is_integer(t)


def _is_float(t: pa.DataType) -> bool:
    return pa.types.is_floating(t)


def _is_decimal(t: pa.DataType) -> bool:
    return pa.types.is_decimal(t)


def _is_temporal(t: pa.DataType) -> bool:
    return (
        pa.types.is_date(t)
        or pa.types.is_timestamp(t)
        or pa.types.is_time(t)
        or pa.types.is_duration(t)
    )


def _widen_arrow(a: pa.DataType, b: pa.DataType) -> pa.DataType | None:  # noqa: C901
    """Return the widened arrow type for (a, b), or None if not applicable."""
    if a == b:
        return a

    # Both null → null
    if pa.types.is_null(a) and pa.types.is_null(b):
        return pa.null()
    if pa.types.is_null(a):
        return b
    if pa.types.is_null(b):
        return a

    # Both signed int
    ra, rb = _int_rank(a), _int_rank(b)
    if ra is not None and rb is not None:
        return _INT_WIDENING[max(ra, rb)]

    # Both unsigned int
    ua, ub = _uint_rank(a), _uint_rank(b)
    if ua is not None and ub is not None:
        return _UINT_WIDENING[max(ua, ub)]

    # Mixed int types: promote to int64
    if _is_integer(a) and _is_integer(b):
        return pa.int64()

    # Both float
    fa, fb = _float_rank(a), _float_rank(b)
    if fa is not None and fb is not None:
        return _FLOAT_WIDENING[max(fa, fb)]

    # Int + float → float64
    if _is_integer(a) and _is_float(b):
        return pa.float64()
    if _is_float(a) and _is_integer(b):
        return pa.float64()

    # Numeric + decimal → decimal128
    if (_is_integer(a) or _is_float(a) or _is_decimal(a)) and (
        _is_integer(b) or _is_float(b) or _is_decimal(b)
    ):
        return pa.decimal128(38, 18)

    # Temporal widening: date32 → timestamp
    if pa.types.is_date32(a) and pa.types.is_date64(b):
        return pa.date64()
    if pa.types.is_date64(a) and pa.types.is_date32(b):
        return pa.date64()
    if pa.types.is_date(a) and pa.types.is_timestamp(b):
        return b
    if pa.types.is_timestamp(a) and pa.types.is_date(b):
        return a
    if pa.types.is_timestamp(a) and pa.types.is_timestamp(b):
        # Prefer higher resolution
        res_order = ["s", "ms", "us", "ns"]
        ra2 = res_order.index(a.unit) if a.unit in res_order else 0
        rb2 = res_order.index(b.unit) if b.unit in res_order else 0
        return a if ra2 >= rb2 else b

    # String absorbs everything (except null, handled above)
    if pa.types.is_string(a) or pa.types.is_large_string(a):
        return pa.string()
    if pa.types.is_string(b) or pa.types.is_large_string(b):
        return pa.string()

    return None  # Cannot widen; caller handles union/JsonBlob


def join(a: "HelixType", b: "HelixType") -> "HelixType":  # noqa: C901
    """Least upper bound. The narrowest type that subsumes both a and b."""
    from helix_ir.types.core import HelixType

    # IDEMPOTENCE
    if a.arrow_type == b.arrow_type and a.semantic == b.semantic:
        total = a.sample_count + b.sample_count
        if total == 0:
            merged_null = 0.0
        else:
            null_count_a = a.null_ratio * a.sample_count
            null_count_b = b.null_ratio * b.sample_count
            merged_null = (null_count_a + null_count_b) / total
        return a.evolve(
            null_ratio=merged_null,
            sample_count=total,
            confidence=min(a.confidence, b.confidence),
        )

    # NULL ABSORPTION
    if pa.types.is_null(a.arrow_type):
        total = a.sample_count + b.sample_count
        null_count = a.sample_count + b.null_ratio * b.sample_count
        nr = null_count / total if total > 0 else 1.0
        return b.evolve(null_ratio=nr, sample_count=total)
    if pa.types.is_null(b.arrow_type):
        total = a.sample_count + b.sample_count
        null_count = b.sample_count + a.null_ratio * a.sample_count
        nr = null_count / total if total > 0 else 1.0
        return a.evolve(null_ratio=nr, sample_count=total)

    total = a.sample_count + b.sample_count
    null_count_a = a.null_ratio * a.sample_count
    null_count_b = b.null_ratio * b.sample_count
    merged_null = (null_count_a + null_count_b) / total if total > 0 else 0.0

    # LIST RECURSIVE
    if pa.types.is_list(a.arrow_type) and pa.types.is_list(b.arrow_type):
        inner_a = HelixType(arrow_type=a.arrow_type.value_type)
        inner_b = HelixType(arrow_type=b.arrow_type.value_type)
        inner_joined = join(inner_a, inner_b)
        return HelixType(
            arrow_type=pa.list_(inner_joined.arrow_type),
            null_ratio=merged_null,
            sample_count=total,
            confidence=min(a.confidence, b.confidence),
        )

    # STRUCT RECURSIVE
    if pa.types.is_struct(a.arrow_type) and pa.types.is_struct(b.arrow_type):
        merged_arrow = _join_struct(a.arrow_type, b.arrow_type)
        return HelixType(
            arrow_type=merged_arrow,
            null_ratio=merged_null,
            sample_count=total,
            confidence=min(a.confidence, b.confidence),
        )

    # If either type is a union semantic, skip regular widening and go straight to union logic
    a_is_union = a.semantic and (a.semantic.startswith("union:") or a.semantic == "json_blob")  # type: ignore[union-attr]
    b_is_union = b.semantic and (b.semantic.startswith("union:") or b.semantic == "json_blob")  # type: ignore[union-attr]

    if not a_is_union and not b_is_union:
        # Try arrow-level widening
        widened = _widen_arrow(a.arrow_type, b.arrow_type)
        if widened is not None:
            return HelixType(
                arrow_type=widened,
                null_ratio=merged_null,
                sample_count=total,
                confidence=min(a.confidence, b.confidence),
            )

    # POLYMORPHIC UNION: wrap in a union type (represented as a tagged dict)
    # We represent this as a JsonBlob if there are too many members
    existing_members = _union_members(a) + _union_members(b)
    # Deduplicate by arrow type string
    seen: set[str] = set()
    unique_members: list["HelixType"] = []
    for m in existing_members:
        key = str(m.arrow_type)
        if key not in seen:
            seen.add(key)
            unique_members.append(m)

    from helix_ir.types.semantic import JSONBLOB_TYPE

    if len(unique_members) > 4:
        return HelixType(
            arrow_type=pa.string(),
            semantic=JSONBLOB_TYPE,
            null_ratio=merged_null,
            sample_count=total,
            confidence=min(a.confidence, b.confidence),
        )

    # Build a union HelixType annotated with semantic='union:<types>'
    member_strs = "|".join(str(m.arrow_type) for m in unique_members)
    return HelixType(
        arrow_type=pa.string(),
        semantic=f"union:{member_strs}",
        null_ratio=merged_null,
        sample_count=total,
        confidence=min(a.confidence, b.confidence),
    )


def _union_members(t: "HelixType") -> list["HelixType"]:
    """If t is a union type, return its members; otherwise return [t]."""
    from helix_ir.types.core import HelixType

    if t.semantic and t.semantic.startswith("union:"):
        parts = t.semantic[6:].split("|")
        return [HelixType(arrow_type=pa.field("x", _parse_arrow_type(p)).type) for p in parts]
    return [t]


def _parse_arrow_type(s: str) -> pa.DataType:
    """Parse a simple arrow type string back to DataType."""
    mapping = {
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
        "bool": pa.bool_(),
        "string": pa.string(),
        "utf8": pa.string(),
        "binary": pa.binary(),
        "null": pa.null(),
        "date32": pa.date32(),
        "date64": pa.date64(),
        "timestamp[us]": pa.timestamp("us"),
        "timestamp[ms]": pa.timestamp("ms"),
        "timestamp[ns]": pa.timestamp("ns"),
        "timestamp[s]": pa.timestamp("s"),
    }
    return mapping.get(s, pa.string())


def _join_struct(a: pa.StructType, b: pa.StructType) -> pa.StructType:
    """Merge two struct Arrow types field by field."""
    from helix_ir.types.core import HelixType

    a_fields = {a.field(i).name: a.field(i).type for i in range(a.num_fields)}
    b_fields = {b.field(i).name: b.field(i).type for i in range(b.num_fields)}

    all_names = list(a_fields.keys())
    for name in b_fields:
        if name not in a_fields:
            all_names.append(name)

    result_fields: list[pa.Field] = []
    for name in all_names:
        if name in a_fields and name in b_fields:
            ta = HelixType(arrow_type=a_fields[name])
            tb = HelixType(arrow_type=b_fields[name])
            merged = join(ta, tb)
            result_fields.append(pa.field(name, merged.arrow_type))
        elif name in a_fields:
            result_fields.append(pa.field(name, a_fields[name]))
        else:
            result_fields.append(pa.field(name, b_fields[name]))

    return pa.struct(result_fields)


def meet(a: "HelixType", b: "HelixType") -> "HelixType":
    """Greatest lower bound — the widest type that fits within both a and b."""
    from helix_ir.types.core import HelixType

    if a.arrow_type == b.arrow_type:
        return a

    # Null is the bottom element
    if pa.types.is_null(a.arrow_type) or pa.types.is_null(b.arrow_type):
        return HelixType(arrow_type=pa.null())

    # For numeric types, take the narrower one
    ra, rb = _int_rank(a.arrow_type), _int_rank(b.arrow_type)
    if ra is not None and rb is not None:
        return a if ra <= rb else b

    fa, fb = _float_rank(a.arrow_type), _float_rank(b.arrow_type)
    if fa is not None and fb is not None:
        return a if fa <= fb else b

    # String is the top; meet with anything → the other
    if pa.types.is_string(a.arrow_type):
        return b
    if pa.types.is_string(b.arrow_type):
        return a

    # Default: null (bottom)
    return HelixType(arrow_type=pa.null())


def subsumes(a: "HelixType", b: "HelixType") -> bool:
    """Return True if any value of type b can be stored in a column of type a.

    In lattice terms: a subsumes b iff join(a, b) == a.
    """
    if a.arrow_type == b.arrow_type:
        return True

    # Null is subsumed by everything
    if pa.types.is_null(b.arrow_type):
        return True

    # String subsumes everything
    if pa.types.is_string(a.arrow_type):
        return True

    # Numeric widening
    ra, rb = _int_rank(a.arrow_type), _int_rank(b.arrow_type)
    if ra is not None and rb is not None:
        return ra >= rb

    fa, fb = _float_rank(a.arrow_type), _float_rank(b.arrow_type)
    if fa is not None and fb is not None:
        return fa >= fb

    # int subsumes float if a is float64
    if a.arrow_type == pa.float64() and _is_integer(b.arrow_type):
        return True

    # decimal subsumes int and float
    if _is_decimal(a.arrow_type) and (_is_integer(b.arrow_type) or _is_float(b.arrow_type)):
        return True

    # Timestamp subsumes date
    if pa.types.is_timestamp(a.arrow_type) and pa.types.is_date(b.arrow_type):
        return True

    # List subsumes list if value type subsumes
    if pa.types.is_list(a.arrow_type) and pa.types.is_list(b.arrow_type):
        from helix_ir.types.core import HelixType
        return subsumes(
            HelixType(arrow_type=a.arrow_type.value_type),
            HelixType(arrow_type=b.arrow_type.value_type),
        )

    return False
