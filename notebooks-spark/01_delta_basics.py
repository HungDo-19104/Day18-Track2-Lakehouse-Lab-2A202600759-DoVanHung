# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # NB1 — Delta Lake Basics
#
# **Mục tiêu:** Tạo Delta table, observe transaction log, demo schema enforcement.
#
# Maps to slide §2 (Delta Lake) + deliverable bullet 1.

# %%
import sys
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from pyspark.sql import functions as F

spark = get_spark("nb1_delta_basics")

# %% [markdown]
# ## 1. Write a Delta table

# %%
data = [
    (1, "alice", 30, "Hanoi"),
    (2, "bob", 25, "HCMC"),
    (3, "charlie", 35, "Danang"),
]
df = spark.createDataFrame(data, ["id", "name", "age", "city"])
table_path = "s3a://lakehouse/users_delta"
df.write.format("delta").mode("overwrite").save(table_path)

# %% [markdown]
# ## 2. Read it back + inspect transaction log
#
# Open MinIO console (http://localhost:9001) → `lakehouse/users_delta/_delta_log/`.
# You should see `00000000000000000000.json`.

# %%
spark.read.format("delta").load(table_path).show()
spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`").show(truncate=False)

# %% [markdown]
# ## 3. Schema enforcement — try to write a wrong schema

# %%
try:
    bad = spark.createDataFrame([(4, "dan", "thirty", "Hue")], ["id", "name", "age", "city"])
    bad.write.format("delta").mode("append").save(table_path)
except Exception as e:
    print("BLOCKED by schema enforcement (expected):")
    print(type(e).__name__, str(e)[:200])

# %% [markdown]
# ## 4. Schema evolution (opt-in)

# %%
new_col = spark.createDataFrame(
    [(4, "dan", 28, "Hue", "premium")],
    ["id", "name", "age", "city", "tier"],
)
new_col.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
spark.read.format("delta").load(table_path).show()

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] `_delta_log/` contains JSON files
# - [ ] Schema enforcement blocked the bad write
# - [ ] mergeSchema added the `tier` column

# %%
spark.stop()
