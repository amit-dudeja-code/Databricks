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

Nice, let’s turn that example into a **fully generic DLT pipeline** that:

* Pulls from a **configurable public API endpoint**
* Handles **dynamic schema per endpoint** via config (no hard‑coded schema)
* Reuses the **LineageAudit** module to track gold‑level lineage back to the API fields

I’ll show:

1. Extra config you need for dynamic public APIs
2. A DLT pipeline script with:

   * Bronze: call public API → raw JSON
   * Silver: config‑driven JSON → columns (dynamic schema)
   * Gold: business logic + lineage / audit

---

## 1. Extra config for public API ingestion

Extend your existing `governance.config.api_endpoints` to carry HTTP details:

```text
governance.config.api_endpoints
--------------------------------------------------------------
endpoint_name        string   -- logical name (e.g. 'gh_repos')
source_system        string   -- e.g. 'GitHub', 'WeatherAPI'
base_url             string   -- e.g. 'https://api.github.com'
resource_path        string   -- e.g. '/repos/databricks/delta'
http_method          string   -- 'GET' (keep simple at first)
query_params_json    string   -- JSON string: '{"per_page":100}'
records_json_path    string   -- where list of records lives, e.g. '$' or '$.items'
bronze_table         string   -- name to use for bronze table
silver_table         string   -- name to use for silver table
gold_table           string   -- name to use for gold table
active_flag          boolean
description          string
```

You already have `config.field_mapping` and `config.business_rules` from the previous answer. For **dynamic schema**, you simply add rows per `(endpoint_name, target_table)`:

* For **silver**: `target_table = silver_<endpoint>`

  * `source_table = bronze_<endpoint>_raw`
  * `source_columns = ['raw_payload']`
  * `transform_expr = "get_json_object(raw_payload, '$.id')"` etc.
* For **gold**: `target_table = gold_<endpoint>`

  * `source_table = silver_<endpoint>`
  * `source_columns = ['repo_id', 'stars', 'forks']`, etc.
  * `transform_expr` is your business logic (`stars + forks`, casting, filters, etc.)

The pipeline code stays generic; **only config changes per endpoint**.

---

## 2. DLT pipeline script with configurable endpoint & dynamic schema

Assume you’ve already deployed `lineage_audit.py` with the `LineageAudit` class from before.

### 2.1 Imports and shared helpers

```python
import dlt
import requests
import json

from pyspark.sql import functions as F

from lineage_audit import LineageAudit

spark = spark  # Databricks runtime SparkSession

# -----------------------------------------------------------------------------
# Pipeline-scoped config
# In real life, set this via the DLT pipeline "Configuration" and read with spark.conf.get.
# e.g. config key: pipeline.endpoint_name = "gh_repos"
# -----------------------------------------------------------------------------
ENDPOINT_NAME = spark.conf.get("pipeline.endpoint_name", "gh_repos")

PIPELINE_NAME = "public_api_ingestion_pipeline"
SOURCE_SYSTEM = "PublicAPI"

# We could also read these from governance.config.api_endpoints,
# but it's handy to build them in a consistent way:
BRONZE_TABLE = f"bronze_{ENDPOINT_NAME}_raw"
SILVER_TABLE = f"silver_{ENDPOINT_NAME}"
GOLD_TABLE = f"gold_{ENDPOINT_NAME}"

la = LineageAudit(spark)


def load_endpoint_config(endpoint_name: str):
    """Load API endpoint metadata (URL, path, params, table names) from Unity Catalog."""
    cfg_df = (
        spark.table("governance.config.api_endpoints")
        .where(F.col("endpoint_name") == endpoint_name)
        .where(F.col("active_flag") == True)
    )
    cfg = cfg_df.first()
    if not cfg:
        raise ValueError(f"No active API endpoint config found for '{endpoint_name}'")

    return cfg


def fetch_api_df(endpoint_name: str):
    """
    Call the public API defined in config.api_endpoints and return a DataFrame
    with a single 'raw_payload' column (one row per record).
    """
    cfg = load_endpoint_config(endpoint_name)

    base_url = cfg.base_url
    resource_path = cfg.resource_path
    http_method = cfg.http_method or "GET"
    params = json.loads(cfg.query_params_json or "{}")
    records_json_path = cfg.records_json_path or "$"

    url = base_url.rstrip("/") + "/" + resource_path.lstrip("/")

    if http_method.upper() != "GET":
        raise NotImplementedError("Example only supports GET for now")

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    # Very simple record extraction logic:
    # - if the result is a list → that's our records
    # - if dict with "items" → use that
    # (you can make this more generic using records_json_path + JSONPath lib)
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and "items" in payload:
        records = payload["items"]
    else:
        records = [payload]

    rows = [(json.dumps(r),) for r in records]

    return spark.createDataFrame(rows, schema="raw_payload string")
```

> 🔑 **Dynamic bit**: the schema of each record is *not* known in code; we just store raw JSON.
> The per‑endpoint schema lives in `config.field_mapping` and is applied later.

---

### 2.2 Bronze table – raw JSON from configurable public API

```python
@dlt.table(
    name=BRONZE_TABLE,
    comment="Raw JSON from configurable public API endpoint",
    table_properties={"quality": "bronze"},
)
def bronze_public_api_raw():
    df_raw = fetch_api_df(ENDPOINT_NAME)

    return (
        df_raw
        .withColumn("endpoint_name", F.lit(ENDPOINT_NAME))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
    )
```

* If you point `ENDPOINT_NAME` at a different row in `config.api_endpoints`, this same function calls another API and writes to a different bronze table (`bronze_other_endpoint_raw`), **no code change**.

---

### 2.3 Silver table – dynamic schema using config.field_mapping

Now we normalize JSON → columns with **no hard‑coded schema**.

In `governance.config.field_mapping` you add rows like:

| endpoint_name | target_table    | target_column | source_table        | source_columns  | transform_expr                                       | api_json_path        |
| ------------- | --------------- | ------------- | ------------------- | --------------- | ---------------------------------------------------- | -------------------- |
| gh_repos      | silver_gh_repos | repo_id       | bronze_gh_repos_raw | ["raw_payload"] | `get_json_object(raw_payload, '$.id')`               | `$.id`               |
| gh_repos      | silver_gh_repos | name          | bronze_gh_repos_raw | ["raw_payload"] | `get_json_object(raw_payload, '$.name')`             | `$.name`             |
| gh_repos      | silver_gh_repos | stars         | bronze_gh_repos_raw | ["raw_payload"] | `get_json_object(raw_payload, '$.stargazers_count')` | `$.stargazers_count` |
| gh_repos      | silver_gh_repos | forks         | bronze_gh_repos_raw | ["raw_payload"] | `get_json_object(raw_payload, '$.forks_count')`      | `$.forks_count`      |
| gh_repos      | silver_gh_repos | language      | bronze_gh_repos_raw | ["raw_payload"] | `get_json_object(raw_payload, '$.language')`         | `$.language`         |

…and similarly for any other endpoint.

Then:

```python
@dlt.table(
    name=SILVER_TABLE,
    comment="Schema-normalized records from public API (dynamic per endpoint)",
    table_properties={"quality": "silver"},
)
def silver_public_api():
    bronze_df = dlt.read(BRONZE_TABLE)

    # Apply mapping based on endpoint + target_table = silver_<endpoint>
    # Note: we don't need lineage persisted for silver; we only care about gold.
    df_silver_with_lineage = la.apply_mapping_with_lineage(
        df_source=bronze_df,
        endpoint_name=ENDPOINT_NAME,
        target_table=SILVER_TABLE,
        primary_key_cols=[],  # no PK requirement at this layer
    )

    # Drop internal __lineage; it's only used if you want silver-level lineage.
    return df_silver_with_lineage.drop("__lineage")
```

> 🔑 **Dynamic schema**: If you switch `ENDPOINT_NAME` from `gh_repos` to `weather_forecast`, the only thing that changes is **which rows** in `config.field_mapping` are loaded. The silver table’s schema becomes whatever `target_column` set you configure there.

---

### 2.4 Gold table – business logic + lineage/audit

Now gold applies business rules on top of silver, and we **capture full lineage** (including back to API fields via `api_json_path` and `source_values_json`).

Assume your gold mapping config looks like:

| endpoint_name | target_table  | target_column  | source_table    | source_columns    | transform_expr                            | business_rule_id | api_field_name           | api_json_path        | is_primary_key |
| ------------- | ------------- | -------------- | --------------- | ----------------- | ----------------------------------------- | ---------------- | ------------------------ | -------------------- | -------------- |
| gh_repos      | gold_gh_repos | repo_id        | silver_gh_repos | ["repo_id"]       | `cast(repo_id as bigint)`                 | `BR_REPO_ID`     | `id`                     | `$.id`               | true           |
| gh_repos      | gold_gh_repos | activity_score | silver_gh_repos | ["stars","forks"] | `cast(stars as int) + cast(forks as int)` | `BR_SCORE`       | `stargazers_count,forks` | `$.stargazers_count` |                |
| gh_repos      | gold_gh_repos | language       | silver_gh_repos | ["language"]      | `upper(language)`                         | `BR_LANG_UPPER`  | `language`               | `$.language`         |                |

Then:

```python
# Helper: get PK columns for gold from mapping config
def get_primary_keys(endpoint_name: str, target_table: str):
    mapping_df = la.load_field_mapping(endpoint_name, target_table)
    pk_rows = mapping_df.where(F.col("is_primary_key") == True).select("target_column").collect()
    return [r["target_column"] for r in pk_rows]


@dlt.table(
    name=GOLD_TABLE,
    comment="Gold table for public API endpoint, with business logic applied",
    table_properties={"quality": "gold"},
)
def gold_public_api():
    silver_df = dlt.read(SILVER_TABLE)

    # Identify PK columns from config
    pk_cols = get_primary_keys(ENDPOINT_NAME, GOLD_TABLE)

    # Start audit run
    run_id = la.start_run(
        pipeline_name=PIPELINE_NAME,
        endpoint_name=ENDPOINT_NAME,
        source_system=SOURCE_SYSTEM,
        target_table=GOLD_TABLE,
    )

    # Apply config-driven transform + attach lineage info
    df_gold_with_lineage = la.apply_mapping_with_lineage(
        df_source=silver_df,
        endpoint_name=ENDPOINT_NAME,
        target_table=GOLD_TABLE,
        primary_key_cols=pk_cols,
    )

    # Basic counts – in real life you'd probably derive these from event logs
    input_count = silver_df.count()
    output_count = df_gold_with_lineage.count()

    # Persist column-level lineage for this run
    la.write_column_lineage(
        df_with_lineage=df_gold_with_lineage,
        run_id=run_id,
        endpoint_name=ENDPOINT_NAME,
        target_table=GOLD_TABLE,
        primary_key_cols=pk_cols,
    )

    # Finalize run audit
    la.finalize_run(
        run_id=run_id,
        status="SUCCESS",
        input_record_count=input_count,
        output_record_count=output_count,
        rejected_record_count=0,
        error_message=None,
    )

    # Expose gold table without internal lineage column
    return df_gold_with_lineage.drop("__lineage")
```

---

## 3. How this satisfies “configurable endpoint + dynamic schema + lineage”

**Configurable endpoint**

* Change **one value** in DLT config: `pipeline.endpoint_name` (or equivalent).
* Change / add row in `governance.config.api_endpoints` for that endpoint.
* The pipeline automatically:

  * Calls the new API URL and parameters.
  * Uses new bronze/silver/gold table names.
  * Uses associated mapping rules for silver + gold.

**Dynamic schema**

* For each endpoint + layer (`silver_<endpoint>`, `gold_<endpoint>`), you define the schema in `config.field_mapping`:

  * `target_column` is the column name.
  * `transform_expr` defines how to get the value (from `raw_payload` JSON or from silver fields).
* No schema is hard‑coded in Python; you can add/remove columns by modifying config.

**Lineage back to API fields and values**

* `LineageAudit.apply_mapping_with_lineage()` adds, for **each row & column**:

  * Which **source columns** were used from the source table (silver).
  * **Source values** used (`source_values_json`).
  * The exact **transform expression**.
  * `business_rule_id` and the upstream **`api_field_name` / `api_json_path`** from config.
* `write_column_lineage()` explodes this into `governance.audit.column_lineage`, giving:

```text
gold table / pk / column
   → which API field name & JSON path
   → which source columns and values
   → which transform expression
   → which business rule id
   → which run_id / timestamp
```

Paired with Unity Catalog’s table/column lineage graph, you can now trace:

> Gold table column → silver columns → bronze raw JSON → API field (name + JSON path)
> plus the exact rule and values used in the transformation for that run.

If you want, we can next add: **validation rules** driven by `business_rules` (e.g. expectations / rejects table) that also log which records failed which rules per endpoint in the same generic way.


If you’d like, I can next:

* Add a **validation / expectations hook** that logs rejected rows with which rule failed.
* Show a **sample set of config rows** for `field_mapping` and `business_rules` so you can bootstrap your UC tables.
