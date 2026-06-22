"""Generate 1M synthetic LLM-observability records and write to MinIO `bronze`.

Schema mirrors slide §8 medallion example. Realism choices match the
lightweight generator so both paths behave identically for grading:

  - Timestamps spread across 7 days → multi-day Gold table.
  - ~5% of `request_id`s reappear (retry pattern) → Silver dedup observable.
  - Latency derived from a per-model profile, not unbounded multiplier.
"""
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

# Make this script runnable both as `python /workspace/scripts/generate_data.py`
# and via `python -m scripts.generate_data` from /workspace.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import Row

from spark_session import get_spark


LATENCY_PROFILES = {
    "claude-haiku-4-5":   (450,  150),
    "claude-sonnet-4-6":  (1100, 350),
    "claude-opus-4-7":    (2400, 700),
}
DUP_RATE = 0.05
DAYS_SPAN = 7


def _build_rows(n_rows: int) -> list[Row]:
    """Build rows in driver — keeps duplicate-injection deterministic.

    1M rows × ~250 B = ~250 MB driver memory; fine for the Docker default."""
    random.seed(42)
    start = datetime(2026, 4, 1, tzinfo=timezone.utc)
    span_seconds = DAYS_SPAN * 24 * 3600
    seen_ids: list[str] = []
    out = []
    for i in range(n_rows):
        ts = start + timedelta(seconds=int(i * span_seconds / n_rows))
        model = random.choices(
            ["claude-sonnet-4-6", "claude-haiku-4-5", "claude-opus-4-7"],
            weights=[6, 3, 1],
        )[0]
        pt = random.randint(50, 4000)
        ct = random.randint(20, 2000)
        base, jitter = LATENCY_PROFILES[model]
        latency_ms = max(50, int((base / 800.0) * ct + random.gauss(0, jitter)))
        status = random.choices(["ok", "rate_limited", "error"], weights=[95, 3, 2])[0]

        if seen_ids and random.random() < DUP_RATE:
            rid = random.choice(seen_ids[-1024:])
        else:
            rid = str(uuid.uuid4())
            seen_ids.append(rid)

        out.append(Row(
            request_id=rid,
            ts=ts,
            raw_json=json.dumps({
                "model": model,
                "user_id": f"u_{random.randint(1, 5000)}",
                "usage": {"input": pt, "output": ct},
                "latency_ms": latency_ms,
                "status": status,
            }),
        ))
    return out


def main(n_rows: int = 1_000_000, out: str = "s3a://bronze/llm_calls_raw") -> None:
    spark = get_spark("generate_data")
    rows = _build_rows(n_rows)
    df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices=16))
    df.write.format("delta").mode("overwrite").save(out)
    n_unique = df.select("request_id").distinct().count()
    print(
        f"Wrote {n_rows:,} rows to {out}\n"
        f"  unique request_ids: {n_unique:,}  ({n_rows - n_unique:,} duplicates seeded)\n"
        f"  date span: {DAYS_SPAN} UTC days from 2026-04-01"
    )
    spark.stop()


if __name__ == "__main__":
    main()
