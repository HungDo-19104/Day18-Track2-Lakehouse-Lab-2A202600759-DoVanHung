"""Smoke test for the lightweight path. Run via `make smoke` or `python scripts/verify_lite.py`."""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import polars as pl
from deltalake import DeltaTable, write_deltalake

from lakehouse import path, reset


def step(label: str) -> None:
    print(f"  • {label}")


def main() -> int:
    print("Lakehouse smoke test (lightweight path)")
    try:
        smoke_path = path("scratch", "_smoke")
        reset(smoke_path)

        step("Write Delta table with delta-rs")
        df = pl.DataFrame({"n": list(range(10))})
        write_deltalake(smoke_path, df.to_arrow(), mode="overwrite")

        step("Read it back")
        n = DeltaTable(smoke_path).to_pyarrow_table().num_rows
        assert n == 10, f"expected 10 rows, got {n}"

        step("Append + verify time travel (v0 still has 10 rows)")
        write_deltalake(smoke_path, df.to_arrow(), mode="append")
        v0 = DeltaTable(smoke_path, version=0).to_pyarrow_table().num_rows
        assert v0 == 10, f"v0 should be 10, got {v0}"

        step("history() shows ≥ 2 versions")
        hist = DeltaTable(smoke_path).history()
        assert len(hist) >= 2, f"expected ≥ 2 versions, got {len(hist)}"

        step("DuckDB can scan the Delta table")
        import duckdb
        rows = duckdb.sql(f"SELECT count(*) FROM delta_scan('{smoke_path}')").fetchone()[0]
        assert rows == 20, f"DuckDB count should be 20, got {rows}"

        print("\nAll checks passed — lightweight lab is ready. Run `make lab`.")
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"\nSmoke test FAILED: {type(e).__name__}: {e}\n")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
