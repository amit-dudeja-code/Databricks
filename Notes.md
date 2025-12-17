Here’s a concrete pattern you can lift into Databricks: a **generic audit + lineage module** (PySpark) that any DLT ingestion pipeline can reuse, with configuration stored in Unity Catalog.

I’ll break it into:

1. **Data model / config tables (in Unity Catalog)**
2. **Audit + lineage tables**
3. **Reusable Python module (`lineage_audit.py`)**
4. **Example usage in a DLT pipeline (bronze → gold → API)**

---

## 1. Config stored in Unity Catalog

Put all config in a central UC catalog, e.g. `governance`:

### 1.1 Endpoint configuration

**Table**: `governance.config.api_endpoints`

| column        | type    | description                                   |
| ------------- | ------- | --------------------------------------------- |
| endpoint_name | string  | Logical name of source API (e.g. `orders_v2`) |
| source_system | string  | e.g. `SAP`, `Salesforce`, `InternalApp`       |
| raw_table     | string  | Bronze table name (DLT)                       |
| active_flag   | boolean | Whether endpoint is currently in use          |
| description   | string  | Human description                             |

### 1.2 Field mapping + transformation config

**Table**: `governance.config.field_mapping`

This is the core “schema + logic mapping” from **bronze (DLT)** → **API-serving (gold)** with references to upstream API fields.

| column           | type          | description                                                             |
| ---------------- | ------------- | ----------------------------------------------------------------------- |
| endpoint_name    | string        | FK to `api_endpoints`                                                   |
| target_layer     | string        | `'gold'`, `'silver'`, `'api_view'` etc.                                 |
| target_table     | string        | e.g. `gold_orders`                                                      |
| target_column    | string        | e.g. `order_amount`                                                     |
| source_table     | string        | Usually bronze or silver table name                                     |
| source_columns   | array<string> | List of column names in `source_table` used in this transformation      |
| api_field_name   | string        | Name of upstream API field                                              |
| api_json_path    | string        | JSON path in raw payload, e.g. `$.order.amount.value`                   |
| transform_expr   | string        | Spark SQL expression based on `source_columns`, e.g. `amount * fx_rate` |
| business_rule_id | string        | FK to business rule (nullable)                                          |
| is_primary_key   | boolean       | Whether this column is part of PK                                       |
| data_type        | string        | Target Spark type (for casting)                                         |
| nullable         | boolean       | Expected nullability                                                    |
| active_flag      | boolean       | Only active mappings are applied                                        |

### 1.3 Business rules config

**Table**: `governance.config.business_rules`

| column           | type   | description                                                              |
| ---------------- | ------ | ------------------------------------------------------------------------ |
| business_rule_id | string | Unique rule id                                                           |
| endpoint_name    | string | Scope to endpoint                                                        |
| rule_category    | string | e.g. `derivation`, `filter`, `validation`, `default`                     |
| rule_expression  | string | SQL predicate or expression (`order_amount > 0`, `coalesce(x,y,0)` etc.) |
| severity         | string | `WARN` / `ERROR` etc.                                                    |
| message          | string | Human-readable description                                               |
| active_flag      | bool   | Enable / disable                                                         |

---

## 2. Audit & lineage tables

### 2.1 Run-level audit

**Table**: `governance.audit.ingestion_run`

| column                | type      | description                    |
| --------------------- | --------- | ------------------------------ |
| run_id                | string    | UUID per pipeline run          |
| pipeline_name         | string    | DLT pipeline / job name        |
| endpoint_name         | string    | FK to `api_endpoints`          |
| source_system         | string    | e.g. `Salesforce`              |
| target_table          | string    | e.g. `gold_orders`             |
| run_start_ts          | timestamp |                                |
| run_end_ts            | timestamp |                                |
| status                | string    | `STARTED`, `SUCCESS`, `FAILED` |
| input_record_count    | long      | From bronze / source           |
| output_record_count   | long      | To gold / API table            |
| rejected_record_count | long      | Failed validations             |
| error_message         | string    | On failure                     |

### 2.2 Column-level lineage (with values used)

We’ll store **column-level + per-row lineage** in a flexible, JSON-friendly shape.

**Table**: `governance.audit.column_lineage`

| column             | type          | description                                                  |
| ------------------ | ------------- | ------------------------------------------------------------ |
| run_id             | string        | FK to `ingestion_run`                                        |
| endpoint_name      | string        |                                                              |
| target_table       | string        |                                                              |
| target_pk          | string        | PK of gold/API record (stringified)                          |
| target_column      | string        | Column in gold/API                                           |
| api_field_name     | string        | Upstream API field                                           |
| api_json_path      | string        |                                                              |
| source_table       | string        |                                                              |
| source_columns     | array<string> |                                                              |
| transform_expr     | string        | Expression used                                              |
| business_rule_id   | string        | Optional                                                     |
| source_values_json | string        | JSON of `{colName: value}` used for this column’s derivation |
| created_ts         | timestamp     |                                                              |

This table is what lets you go: **gold record/column → which API field(s) and values created it, under which logic**.

---

## 3. Generic Python module: `lineage_audit.py`

Below is a reusable PySpark module you can put in a shared repo / wheel and import into any Databricks/ DL pipeline.

```python
# lineage_audit.py

import uuid
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


class LineageAudit:
    """
    Generic audit + lineage helper for Databricks pipelines.
    - Uses Unity Catalog tables for config and audit.
    - Works for any endpoint via config.
    """

    def __init__(
        self,
        spark,
        config_catalog: str = "governance",
        config_schema: str = "config",
        audit_schema: str = "audit"
    ):
        self.spark = spark
        self.config_catalog = config_catalog
        self.config_schema = config_schema
        self.audit_schema = audit_schema

        self.config_db = f"{config_catalog}.{config_schema}"
        self.audit_db = f"{config_catalog}.{audit_schema}"

    # -------------------------------------------------------------------------
    # Audit run helpers
    # -------------------------------------------------------------------------
    def start_run(
        self,
        pipeline_name: str,
        endpoint_name: str,
        source_system: str,
        target_table: str
    ) -> str:
        run_id = str(uuid.uuid4())
        now = datetime.utcnow()

        run_df = self.spark.createDataFrame(
            [
                (
                    run_id,
                    pipeline_name,
                    endpoint_name,
                    source_system,
                    target_table,
                    now,
                    None,
                    "STARTED",
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=T.StructType(
                [
                    T.StructField("run_id", T.StringType(), False),
                    T.StructField("pipeline_name", T.StringType(), False),
                    T.StructField("endpoint_name", T.StringType(), False),
                    T.StructField("source_system", T.StringType(), False),
                    T.StructField("target_table", T.StringType(), False),
                    T.StructField("run_start_ts", T.TimestampType(), False),
                    T.StructField("run_end_ts", T.TimestampType(), True),
                    T.StructField("status", T.StringType(), False),
                    T.StructField("input_record_count", T.LongType(), True),
                    T.StructField("output_record_count", T.LongType(), True),
                    T.StructField("rejected_record_count", T.LongType(), True),
                    T.StructField("error_message", T.StringType(), True),
                ]
            ),
        )

        run_df.write.format("delta").mode("append").saveAsTable(
            f"{self.audit_db}.ingestion_run"
        )

        return run_id

    def finalize_run(
        self,
        run_id: str,
        status: str,
        input_record_count: int,
        output_record_count: int,
        rejected_record_count: int = 0,
        error_message: str = None,
    ):
        """
        Update the run row. For simplicity we overwrite the row by run_id.
        """

        now = datetime.utcnow()

        # Read existing run row
        run_table = f"{self.audit_db}.ingestion_run"
        run_df = self.spark.table(run_table).where(F.col("run_id") == run_id)

        updated_df = (
            run_df.drop("run_end_ts", "status", "input_record_count",
                        "output_record_count", "rejected_record_count",
                        "error_message")
            .withColumn("run_end_ts", F.lit(now).cast("timestamp"))
            .withColumn("status", F.lit(status))
            .withColumn("input_record_count", F.lit(input_record_count))
            .withColumn("output_record_count", F.lit(output_record_count))
            .withColumn("rejected_record_count", F.lit(rejected_record_count))
            .withColumn("error_message", F.lit(error_message))
        )

        # Overwrite this run_id row
        (
            updated_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"run_id = '{run_id}'")
            .saveAsTable(run_table)
        )

    # -------------------------------------------------------------------------
    # Config loaders
    # -------------------------------------------------------------------------
    def load_field_mapping(self, endpoint_name: str, target_table: str) -> DataFrame:
        """
        Load mapping config for specific endpoint + target_table.
        """
        mapping_table = f"{self.config_db}.field_mapping"

        return (
            self.spark.table(mapping_table)
            .where(F.col("endpoint_name") == endpoint_name)
            .where(F.col("target_table") == target_table)
            .where(F.col("active_flag") == F.lit(True))
        )

    def load_business_rules(self, endpoint_name: str) -> DataFrame:
        rules_table = f"{self.config_db}.business_rules"
        return (
            self.spark.table(rules_table)
            .where(F.col("endpoint_name") == endpoint_name)
            .where(F.col("active_flag") == F.lit(True))
        )

    # -------------------------------------------------------------------------
    # Core: apply mapping with embedded lineage column
    # -------------------------------------------------------------------------
    def apply_mapping_with_lineage(
        self,
        df_source: DataFrame,
        endpoint_name: str,
        target_table: str,
        primary_key_cols: list[str],
    ) -> DataFrame:
        """
        Build the gold/api DataFrame using config-driven mappings.
        Returns a DataFrame with:
           - target columns
           - a __lineage map column: target_col -> {source_columns, source_values, transform_expr}
        """

        mapping_df = self.load_field_mapping(endpoint_name, target_table)
        mappings = mapping_df.collect()  # safe: config is small

        select_exprs = []
        lineage_entries = []

        # ensure PK columns are carried over explicitly
        for pk in primary_key_cols:
            if pk not in df_source.columns:
                raise ValueError(f"Primary key column {pk} missing in source df")
            select_exprs.append(F.col(pk).alias(pk))

        for m in mappings:
            tgt_col = m["target_column"]
            src_cols = m["source_columns"] or []
            transform_expr = m["transform_expr"]
            data_type = m["data_type"]
            nullable = m["nullable"]

            # Build expression for target column
            col_expr = F.expr(transform_expr)
            if data_type:
                col_expr = col_expr.cast(data_type)

            select_exprs.append(col_expr.alias(tgt_col))

            # lineage: struct with source columns, source values, transform, business rule id, api info
            source_values = [
                F.struct(
                    F.lit(c).alias("column_name"),
                    F.col(c).alias("value")
                )
                for c in src_cols
            ]

            lineage_value_struct = F.struct(
                F.array(*[F.lit(c) for c in src_cols]).alias("source_columns"),
                F.array(*source_values).alias("source_values"),
                F.lit(transform_expr).alias("transform_expr"),
                F.lit(m["business_rule_id"]).alias("business_rule_id"),
                F.lit(m["api_field_name"]).alias("api_field_name"),
                F.lit(m["api_json_path"]).alias("api_json_path"),
            )

            lineage_entries.append(
                F.struct(
                    F.lit(tgt_col).alias("key"),
                    lineage_value_struct.alias("value"),
                )
            )

        df_out = df_source.select(*select_exprs)

        # Add map: target_column -> lineage info
        if lineage_entries:
            df_out = df_out.withColumn(
                "__lineage",
                F.map_from_entries(F.array(*lineage_entries))
            )
        else:
            df_out = df_out.withColumn("__lineage", F.map_from_entries(F.array()))

        return df_out

    # -------------------------------------------------------------------------
    # Flatten lineage map to audit table
    # -------------------------------------------------------------------------
    def write_column_lineage(
        self,
        df_with_lineage: DataFrame,
        run_id: str,
        endpoint_name: str,
        target_table: str,
        primary_key_cols: list[str],
    ):
        """
        Explode __lineage map and write per-row, per-column lineage to audit table.
        """

        pk_expr = F.concat_ws("||", *[F.col(c).cast("string") for c in primary_key_cols])

        exploded = (
            df_with_lineage
            .withColumn("target_pk", pk_expr)
            .select(
                "target_pk",
                "__lineage",
            )
            .withColumn("kv", F.explode("__lineage"))
            .select(
                F.lit(run_id).alias("run_id"),
                F.lit(endpoint_name).alias("endpoint_name"),
                F.lit(target_table).alias("target_table"),
                F.col("target_pk"),
                F.col("kv.key").alias("target_column"),
                F.col("kv.value.api_field_name").alias("api_field_name"),
                F.col("kv.value.api_json_path").alias("api_json_path"),
                F.col("kv.value.source_columns").alias("source_columns"),
                F.col("kv.value.transform_expr").alias("transform_expr"),
                F.col("kv.value.business_rule_id").alias("business_rule_id"),
                # Convert array<struct<column_name,value>> to JSON {colName: value}
                F.to_json(
                    F.map_from_entries(
                        F.col("kv.value.source_values")
                        .cast(
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("key", T.StringType()),
                                        T.StructField("value", T.StringType()),
                                    ]
                                )
                            )
                        )
                    )
                ).alias("source_values_json"),
                F.current_timestamp().alias("created_ts"),
            )
        )

        (
            exploded.write.format("delta")
            .mode("append")
            .saveAsTable(f"{self.audit_db}.column_lineage")
        )
```

This module gives you:

* **Run lifecycle**: `start_run()` and `finalize_run()`
* **Config-driven transformation**: `apply_mapping_with_lineage()`
* **Row + column lineage persistence**: `write_column_lineage()`

---

## 4. Example: using it in a DLT pipeline

Imagine a DLT pipeline that ingests data from an API into a bronze table `bronze_orders_raw`, then normalizes to `silver_orders`, and finally builds `gold_orders` that serves the `GET /orders` API.

### 4.1 DLT gold table with lineage / audit

```python
import dlt
from pyspark.sql import functions as F

from lineage_audit import LineageAudit

spark = spark  # Databricks default
la = LineageAudit(spark)

PIPELINE_NAME = "orders_ingestion_pipeline"
ENDPOINT_NAME = "orders_v2"
SOURCE_SYSTEM = "InternalOrdersService"
TARGET_TABLE = "gold_orders"
PRIMARY_KEYS = ["order_id"]


@dlt.table(
    name=TARGET_TABLE,
    comment="Gold orders table for Orders API",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def gold_orders():
    # 1. Read from silver (or bronze if you prefer)
    df_silver = dlt.read("silver_orders")

    # 2. Start audit run (can be done outside the function in some setups)
    run_id = la.start_run(
        pipeline_name=PIPELINE_NAME,
        endpoint_name=ENDPOINT_NAME,
        source_system=SOURCE_SYSTEM,
        target_table=TARGET_TABLE,
    )

    # 3. Apply mapping + lineage
    df_gold = la.apply_mapping_with_lineage(
        df_source=df_silver,
        endpoint_name=ENDPOINT_NAME,
        target_table=TARGET_TABLE,
        primary_key_cols=PRIMARY_KEYS,
    )

    # 4. Basic counts for audit
    input_count = df_silver.count()
    output_count = df_gold.count()

    # 5. Write lineage out-of-band (in production you’d usually do this in a separate step
    #    or a DLT expectation action to avoid extra passes over the data)
    la.write_column_lineage(
        df_with_lineage=df_gold,
        run_id=run_id,
        endpoint_name=ENDPOINT_NAME,
        target_table=TARGET_TABLE,
        primary_key_cols=PRIMARY_KEYS,
    )

    # 6. Finalize run
    la.finalize_run(
        run_id=run_id,
        status="SUCCESS",
        input_record_count=input_count,
        output_record_count=output_count,
        rejected_record_count=0,
        error_message=None,
    )

    # 7. Return df for DLT table (without the internal __lineage column)
    return df_gold.drop("__lineage")
```

> If you want to **expose lineage to downstream consumers**, you can keep `__lineage` as a hidden / internal column, or materialize it into a dedicated `gold_orders_lineage` table.

---

## 5. How this gives you “gold → API field + values + logic” trace

For any record in `gold_orders`:

1. **Find its PK + column** (e.g. `order_id = 12345`, column `order_amount`).

2. Query `governance.audit.column_lineage`:

   ```sql
   SELECT *
   FROM governance.audit.column_lineage
   WHERE target_table = 'gold_orders'
     AND target_pk = '12345'
     AND target_column = 'order_amount';
   ```

   You’ll get:

   * `api_field_name`: e.g. `amount.value`
   * `api_json_path`: e.g. `$.order.amount.value`
   * `source_columns`: e.g. `["amount_raw", "fx_rate"]`
   * `transform_expr`: e.g. `amount_raw * fx_rate`
   * `source_values_json`: e.g. `{"amount_raw": "100", "fx_rate": "1.07"}`
   * `business_rule_id`: for cross-reference to rules

3. Join to `governance.config.business_rules` on `business_rule_id` to see the business meaning and rule description.

Combined with Unity Catalog’s built-in **table-level lineage**, this gives you:

* UC lineage: **which tables / notebooks / pipelines** produced `gold_orders`.
* Custom lineage: **per-row, per-column logic and values linking back to upstream API fields**.

---

If you’d like, I can next:

* Add a **validation / expectations hook** that logs rejected rows with which rule failed.
* Show a **sample set of config rows** for `field_mapping` and `business_rules` so you can bootstrap your UC tables.
