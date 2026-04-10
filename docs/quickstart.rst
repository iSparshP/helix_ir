Quick Start
===========

Installation
------------

.. code-block:: bash

   pip install helix-ir

   # Optional extras
   pip install helix-ir[mongo]     # MongoDB source
   pip install helix-ir[postgres]  # PostgreSQL source
   pip install helix-ir[kafka]     # Kafka source

Hello World: Infer → Normalize → DDL
--------------------------------------

.. code-block:: python

   from helix_ir import infer, normalize, compile_ddl
   from helix_ir.sources import JsonLinesSource

   # Point at a nested source
   source = JsonLinesSource("orders.jsonl")
   docs = source.stream()

   # Infer a typed schema
   schema = infer(docs, name="orders", sample_size=2000, detect_pii=True)
   print(schema)

   # Decompose into a relational plan
   plan = normalize(schema, strategy="1nf")
   print(f"{len(plan.tables)} tables, {len(plan.foreign_keys)} foreign keys")

   # Compile to Postgres DDL
   ddl = compile_ddl(plan, dialect="postgres")
   print(ddl.to_sql())

Transformation Pipeline
-----------------------

.. code-block:: python

   from helix_ir import col
   from helix_ir.sources import JsonLinesSource
   from helix_ir.transform import Table

   orders = Table.from_source(JsonLinesSource("orders.jsonl"))
   items  = Table.from_source(JsonLinesSource("items.jsonl"))

   order_summaries = (
       orders
         .join(items, on="order_id", how="left")
         .with_columns(
             month      = col("created_at").date_trunc("month"),
             line_total = col("qty") * col("unit_price"),
         )
         .group_by("month")
         .agg(revenue=col("line_total").sum(), orders=col("order_id").count())
         .sort("month")
   )

   print(order_summaries.to_sql(dialect="postgres"))
