# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup and Bronze ingestion
# MAGIC **Retail Lakehouse Capstone, Part 1**
# MAGIC
# MAGIC What this notebook does:
# MAGIC 1. Creates a schema and a *volume* (a governed folder for files) in Unity Catalog.
# MAGIC 2. Uses **Auto Loader** to ingest the daily CSV files from the volume into **Bronze** Delta tables.
# MAGIC 3. Lets you verify what happened and see how schema drift is handled.
# MAGIC
# MAGIC **Bronze rule:** keep the data exactly as it arrived. No cleaning, no dedupe, no type casting.
# MAGIC Cleaning belongs in Silver. That way, if a Silver rule is wrong, you can always replay from Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parameters
# MAGIC Unity Catalog names objects as `catalog.schema.object`.
# MAGIC Open **Catalog** in the left sidebar and check the catalog you have. In Free Edition it is
# MAGIC commonly called `workspace`. If yours differs, change `CATALOG` below.
# MAGIC The schema acts as our "environment": `retail_dev` now, `retail_prod` later.

# COMMAND ----------

CATALOG = "workspace"     # change if your catalog name is different
SCHEMA = "retail_dev"
VOLUME = "landing"

# Files in a volume are addressed with this path format:
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("Landing base path:", BASE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the schema, the volume, and the folders
# MAGIC `IF NOT EXISTS` makes this cell **idempotent**: running it twice is safe.
# MAGIC Keep that habit. Idempotency is one of the most asked-about ideas in data engineering interviews.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# One folder per source, plus a _meta folder that Auto Loader uses for its own bookkeeping.
for sub in ["pos", "products", "stores", "_meta/schemas", "_meta/checkpoints"]:
    dbutils.fs.mkdirs(f"{BASE}/{sub}")

display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## >>> STOP HERE: upload your first files <<<
# MAGIC In the left sidebar: **Catalog > workspace > retail_dev > landing**, open the folder `pos`,
# MAGIC and use **Upload to this volume** to upload `pos_2026-10-01.csv`, `pos_2026-10-02.csv`,
# MAGIC `pos_2026-10-03.csv`. Do the same for `products_*` into `products` and `stores_*` into `stores`.
# MAGIC (Days 1 to 3 only. We keep days 4 and later for the drift experiment.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: The Bronze ingestion function
# MAGIC
# MAGIC **Why Auto Loader (`cloudFiles`) instead of `spark.read.csv`?**
# MAGIC `spark.read.csv(folder)` re-reads *every* file each time. Auto Loader remembers which files it has
# MAGIC already processed (in the **checkpoint**) and only picks up new ones. That is what makes the pipeline
# MAGIC incremental and cheap as the folder grows.
# MAGIC
# MAGIC Line by line:
# MAGIC * `cloudFiles.format = csv` : the file type to read.
# MAGIC * `cloudFiles.schemaLocation` : where Auto Loader stores the schema it inferred, so it can compare later.
# MAGIC * `cloudFiles.inferColumnTypes = false` : keep every column as a string in Bronze. Typing happens in Silver.
# MAGIC * `cloudFiles.schemaEvolutionMode = addNewColumns` : if a new column shows up, add it instead of silently dropping it.
# MAGIC * `_ingest_ts`, `_source_file` : audit columns. Data lineage starts with knowing *which file* a row came from.
# MAGIC * `trigger(availableNow=True)` : process everything new, then stop. Batch-style run of a streaming engine.
# MAGIC * `checkpointLocation` : the memory of the stream (which files are done). **Never share one between two streams.**

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_bronze(source_name: str, target_table: str) -> None:
    """Incrementally load new CSV files from BASE/<source_name> into a Bronze Delta table."""
    source_path = f"{BASE}/{source_name}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{BASE}/_meta/schemas/{source_name}")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", f"{BASE}/_meta/checkpoints/{source_name}")
        .option("mergeSchema", "true")     # allow the Delta table to gain new columns
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.{target_table}")
    )
    query.awaitTermination()
    print(f"Done: {source_name} -> {CATALOG}.{SCHEMA}.{target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run it
# MAGIC First run: reads the 3 files per source. **Run this cell a second time right after:**
# MAGIC nothing new should load. That is the checkpoint doing its job.

# COMMAND ----------

ingest_bronze("pos", "bronze_pos")
ingest_bronze("products", "bronze_products")
ingest_bronze("stores", "bronze_stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify (do not skip. Checking your own pipeline is part of the job)

# COMMAND ----------

# Rows per source file. Compare with `wc -l` of the CSVs on your laptop (minus 1 header line each).
display(spark.sql(f"""
    SELECT _source_file, count(*) AS row_count
    FROM {CATALOG}.{SCHEMA}.bronze_pos
    GROUP BY _source_file
    ORDER BY _source_file
"""))

# COMMAND ----------

# Bronze keeps problems as-is. You should SEE the duplicates here (we remove them in Silver).
display(spark.sql(f"""
    SELECT txn_id, line_no, count(*) AS copies
    FROM {CATALOG}.{SCHEMA}.bronze_pos
    GROUP BY txn_id, line_no
    HAVING count(*) > 1
    ORDER BY copies DESC
    LIMIT 20
"""))

# COMMAND ----------

# Bad rows are also still here: missing product_id, or a negative price. Silver will quarantine them.
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.{SCHEMA}.bronze_pos
    WHERE product_id IS NULL OR product_id = '' OR CAST(unit_price AS DOUBLE) < 0
    LIMIT 20
"""))

# COMMAND ----------

# Every write to a Delta table is a versioned transaction. This is the Delta transaction log you will be asked about.
display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.bronze_pos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Schema drift experiment (do this after Steps 1 to 5 work)
# MAGIC 1. Upload `pos_2026-10-04.csv` into the `pos` folder. This file has a **new column, `promo_id`**.
# MAGIC 2. Re-run the Step 4 cell. **Expect it to fail** with an unknown/new-fields error.
# MAGIC    That is by design: in `addNewColumns` mode Auto Loader stops the stream, records the new schema, and
# MAGIC    waits for you (or a job retry) to run again.
# MAGIC 3. Re-run the Step 4 cell again. It should succeed now.
# MAGIC 4. Run the cell below. Older rows should show `NULL` in `promo_id`.
# MAGIC
# MAGIC Think about this: **why would failing loudly be better than silently ignoring the new column?**

# COMMAND ----------

display(spark.sql(f"""
    SELECT _source_file,
           count(*)        AS row_count,
           count(promo_id) AS rows_with_promo_id
    FROM {CATALOG}.{SCHEMA}.bronze_pos
    GROUP BY _source_file
    ORDER BY _source_file
"""))
