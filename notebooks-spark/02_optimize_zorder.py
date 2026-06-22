# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # NB2 — Small-File Problem & OPTIMIZE + ZORDER
#
# **Mục tiêu:** prove the 3–10× speedup claim from slide §6 (Storage Optimization).
# Maps to deliverable bullet 2.

# %%
import sys, time, random
sys.path.append("/workspace/scripts")
from spark_session import get_spark
from delta.tables import DeltaTable

spark = get_spark("nb2_optimize_zorder")
path = "s3a://lakehouse/events_smallfiles"

# %% [markdown]
# ## 0. Reset path (idempotent re-run)
#
# Each run starts fresh — otherwise repeated appends keep growing the table
# and the benchmark drifts.

# %%
spark.sql(f"DROP TABLE IF EXISTS delta.`{path}`")
# Best-effort: the DROP above unregisters the catalog entry, but Delta files
# may persist in MinIO. Overwrite below resets the data.

# %% [markdown]
# ## 1. Manufacture the small-file problem
#
# Append 200 tiny batches → 200 small files. Realistic streaming-ingestion shape.

# %%
for batch in range(200):
    rows = [(i, random.choice(["click", "view", "scroll", "purchase"]),
             random.randint(1, 10000))
            for i in range(batch * 500, (batch + 1) * 500)]
    df = spark.createDataFrame(rows, ["event_id", "kind", "user_id"])
    mode = "overwrite" if batch == 0 else "append"
    df.write.format("delta").mode(mode).save(path)

# %% [markdown]
# ## 2. Benchmark BEFORE optimize

# %%
def bench(label):
    # Warm-up read so we measure query, not cold metadata fetch
    spark.read.format("delta").load(path).limit(1).count()
    t0 = time.time()
    n = (spark.read.format("delta").load(path)
            .where("user_id = 4242 AND kind = 'purchase'").count())
    dt = time.time() - t0
    print(f"{label:25s}  count={n}  time={dt:.2f}s")
    return dt

before = bench("BEFORE OPTIMIZE+ZORDER")

# %% [markdown]
# ## 3. OPTIMIZE + ZORDER

# %%
spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (user_id)")

# %% [markdown]
# ## 4. Benchmark AFTER

# %%
after = bench("AFTER OPTIMIZE+ZORDER")
speedup = before / max(after, 1e-6)
print(f"\nSpeedup: {speedup:.1f}×  (target ≥ 3×)")

# %% [markdown]
# ## 5. Inspect file count change

# %%
spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select(
    "numFiles", "sizeInBytes"
).show()

# %% [markdown]
# ## ✅ Deliverable check
# - [ ] Speedup ≥ 3×
# - [ ] `numFiles` dropped substantially after OPTIMIZE
# - [ ] Screenshot the printed comparison

# %%
spark.stop()
