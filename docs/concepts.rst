Concepts
========

The Universal Intermediate Representation
------------------------------------------

``helix_ir`` represents all data in one universal intermediate representation:
a typed, lineage-aware, schema-annotated DAG. This collapses the modern data
stack from four tools (ingestion, transformation, DDL, lineage) into one library.

The Type System
---------------

Every value is classified into a :class:`~helix_ir.types.HelixType` — an
Apache Arrow logical type extended with Helix metadata:

- **null_ratio** — observed fraction of nulls
- **cardinality_estimate** — HyperLogLog estimate of unique values
- **confidence** — how certain the inference engine is about this type
- **pii_class** — PII classification (email, phone, PAN, etc.)
- **semantic** — semantic hint (email, url, uuid, enum)

The Type Lattice
----------------

When the inference engine sees the same field take two different types across
documents, it merges them using the **type lattice**:

- ``join(Int32, Int64)`` → ``Int64`` (numeric widening)
- ``join(String, Int64)`` → ``String`` (string fallback)
- ``join(Struct, Struct)`` → merged ``Struct`` (recursive)
- ``join(T1, T2)`` → ``Union(T1, T2)`` if no rule applies

The Schema
----------

A :class:`~helix_ir.schema.Schema` is a named, ordered, immutable collection
of ``(field_name, HelixType)`` pairs. Schemas are hashable, JSON-serializable,
and Arrow-compatible.

Normalization Strategies
------------------------

+----------------+--------------------------------------------------------+
| Strategy       | Behaviour                                              |
+================+========================================================+
| ``1nf``        | Strict first normal form — all arrays and complex      |
|                | structs become child tables with foreign keys          |
+----------------+--------------------------------------------------------+
| ``mongo``      | Structs stored as JSON blobs, arrays split             |
+----------------+--------------------------------------------------------+
| ``inline_small``| Structs with ≤ N leaves inlined; larger ones split    |
+----------------+--------------------------------------------------------+
| ``custom``     | Caller-supplied rules per path                         |
+----------------+--------------------------------------------------------+
