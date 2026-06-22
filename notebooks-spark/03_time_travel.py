# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB3 — Time Travel + MERGE Upsert
#
# **Mục tiêu:** demo 4 demo-block tasks from slide line ~700.
# Maps to deliverable bullet 3.

# %%
import sys, time, random
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark = get_spark("nb3_time_travel")
path = "s3a://lakehouse/customers_tt"

# %% [markdown]
# ## 1. Build version history
# v0: initial 100K customers · v1: schema add · v2: MERGE upsert 100K · v3: bad data ingest

# %%
v0 = spark.range(100_000).select(
    F.col("id").alias("customer_id"),
    F.lit("active").alias("status"),
    (F.col("id") % 1000).cast("int").alias("score"),
)
v0.write.format("delta").mode("overwrite").save(path)             # v0

# v1 — add column
df1 = (spark.read.format("delta").load(path)
       .withColumn("tier", F.when(F.col("score") > 800, "gold").otherwise("silver")))
df1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)  # v1

# v2 — MERGE upsert 100K
updates = spark.range(50_000, 150_000).select(
    F.col("id").alias("customer_id"),
    F.lit("vip").alias("status"),
    F.lit(999).alias("score"),
    F.lit("platinum").alias("tier"),
)
target = DeltaTable.forPath(spark, path)
t0 = time.time()
(target.alias("t").merge(updates.alias("s"), "t.customer_id = s.customer_id")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())                                                # v2
print(f"MERGE 100K rows: {time.time()-t0:.2f}s")

# v3 — simulate bad data
bad = spark.range(50).select(F.col("id").alias("customer_id"),
                              F.lit(None).cast("string").alias("status"),
                              F.lit(-1).alias("score"),
                              F.lit("UNKNOWN").alias("tier"))
bad.write.format("delta").mode("append").save(path)               # v3

# %% [markdown]
# ## 2. DESCRIBE HISTORY — audit trail

# %%
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# %% [markdown]
# ## 3. Time travel queries

# %%
print("v0:", spark.read.format("delta").option("versionAsOf", 0).load(path).count())
print("v1 schema:", spark.read.format("delta").option("versionAsOf", 1).load(path).columns)

# %% [markdown]
# ## 4. RESTORE bad version

# %%
t0 = time.time()
DeltaTable.forPath(spark, path).restoreToVersion(2)
print(f"RESTORE: {time.time()-t0:.2f}s   (target < 30s)")

# Verify the bad rows are gone
bad_count = (spark.read.format("delta").load(path)
             .where("score < 0").count())
print(f"Rows with score<0 after restore: {bad_count}  (expected 0)")

# %% [markdown]
# ## 5. Final history — now includes the RESTORE row
#
# (Earlier `DESCRIBE HISTORY` ran *before* the RESTORE, so it would have
# shown only 4 versions. This second pass is the one to screenshot.)

# %%
final = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "operation").collect()
for r in final:
    print(f"  v{r['version']:>2}  {r['operation']}")
print(f"\nTotal versions: {len(final)}  (target ≥ 5)")

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] DESCRIBE HISTORY shows ≥ 5 versions (incl. RESTORE itself)
# - [ ] MERGE 100K finished in < 60s
# - [ ] RESTORE finished in < 30s and removed bad rows

# %%
spark.stop()
