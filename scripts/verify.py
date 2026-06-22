"""End-to-end smoke test. Run via `make smoke`.

Checks:
  1. Spark session boots with Delta extensions.
  2. S3A reaches MinIO and reads/writes the `lakehouse` bucket.
  3. Delta transaction log appears (ACID write succeeded).
  4. Time-travel API works (versionAsOf).
"""
from __future__ import annotations

import sys
import traceback
from spark_session import get_spark


def step(label: str) -> None:
    print(f"  • {label}")


def main() -> int:
    print("Lakehouse smoke test")
    try:
        step("Boot Spark with Delta")
        spark = get_spark("smoke")

        step("Write Delta table to MinIO (s3a://lakehouse/_smoke)")
        df = spark.range(10).withColumnRenamed("id", "n")
        path = "s3a://lakehouse/_smoke"
        df.write.format("delta").mode("overwrite").save(path)

        step("Read it back")
        n = spark.read.format("delta").load(path).count()
        assert n == 10, f"expected 10 rows, got {n}"

        step("Append + verify time travel (v0 still has 10 rows)")
        df.write.format("delta").mode("append").save(path)
        v0 = spark.read.format("delta").option("versionAsOf", 0).load(path).count()
        assert v0 == 10, f"v0 should be 10, got {v0}"

        step("DESCRIBE HISTORY shows ≥ 2 versions")
        history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
        assert len(history) >= 2, f"expected ≥ 2 versions, got {len(history)}"

        spark.stop()
        print("\nAll checks passed — lab is ready. Open http://localhost:8888")
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"\nSmoke test FAILED: {type(e).__name__}: {e}\n")
        traceback.print_exc()
        print(
            "\nCommon causes:\n"
            "  - First run downloading Maven JARs (~200 MB): retry once.\n"
            "  - MinIO not ready: `make logs` and look for 'Buckets ready'.\n"
            "  - Network blocks Maven Central: see README troubleshooting."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
