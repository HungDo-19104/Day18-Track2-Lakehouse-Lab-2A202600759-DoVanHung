# Day 18 Lab — Grading Rubric (100 pts)

Maps 1-to-1 to the slide deliverable (4 bullets). Track-2 Daily Lab weight = 30%.

The lab supports two paths (lightweight `deltalake` vs Spark/Docker). Both
write the **same on-disk Delta format**, so each criterion below accepts
evidence from either path:

- *MinIO `_delta_log/` visible* (Spark path) ↔ *`_lakehouse/.../_delta_log/` visible on local disk* (lightweight path)
- *Spark `OPTIMIZE ... ZORDER BY`* ↔ *`dt.optimize.compact()` + `dt.optimize.z_order(...)`*
- *Spark `MERGE INTO`* ↔ *`dt.merge(...).when_matched_update_all().execute()`*

Submit screenshot + notebook output for each criterion.

| # | Notebook | Criterion (path-agnostic) | Pts |
|---|---|---|---:|
| 1 | `01_delta_basics` | Delta table created; `_delta_log/` JSON files visible (MinIO console **or** local FS) | 10 |
| 1 | `01_delta_basics` | Schema enforcement blocks the `age=str` write | 5 |
| 1 | `01_delta_basics` | `schema_mode="merge"` (or `mergeSchema=true`) adds the `tier` column | 5 |
| 2 | `02_optimize_zorder` | Reproduce small-file problem (≥ 100 files before OPTIMIZE) | 5 |
| 2 | `02_optimize_zorder` | OPTIMIZE+ZORDER speedup ≥ 3× **or** files-pruned ratio ≥ 10× (delta-rs `to_pyarrow_table(filters=...)` bench) | 15 |
| 2 | `02_optimize_zorder` | `numFiles` decreased meaningfully after OPTIMIZE | 5 |
| 3 | `03_time_travel` | `history()` shows ≥ 5 versions (including the RESTORE) | 5 |
| 3 | `03_time_travel` | MERGE upsert 100K rows succeeds in < 60 s | 10 |
| 3 | `03_time_travel` | RESTORE rolls back bad data in < 30 s, verified `score < 0` count = 0 | 10 |
| 4 | `04_medallion`    | Bronze, Silver, Gold tables all exist on the storage layer of your path | 10 |
| 4 | `04_medallion`    | Silver dedup measurably drops rows (Silver < Bronze count) | 5 |
| 4 | `04_medallion`    | Gold aggregations correct (p50/p95 latency, cost_usd, error_rate) for ≥ 7 dates | 10 |
| — | All notebooks     | Reproducible from clean `make setup && make smoke` (lite) or `make spark-up && make spark-smoke` (docker) | 5 |
|   |                   | **Core total** | **100** |

## Bonus Challenge (optional, ungraded)

Open-ended architecture brief — see
[`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md) (tiếng Việt) /
[`BONUS-CHALLENGE-EN.md`](BONUS-CHALLENGE-EN.md) (English)
for the full brief, recommended topics, and a self-checklist of what strong
submissions look like.

The bonus does **not** affect your core grade. Submissions get a substantive
written review from the instructor focusing on judgment and tradeoff
reasoning. Treat it as a portfolio piece, not a credit grab.

## Submission

Push your work to: `<your-username>/Day18-Track2-Lakehouse-Lab` (fork) and open a
PR back to upstream. Include:

1. The 4 executed notebooks (committed with output cells preserved)
2. `submission/screenshots/` — at least one of:
   - MinIO console showing `_delta_log/` + bucket layout (Spark path), OR
   - Terminal `tree _lakehouse/` + a screenshot of one `_delta_log/*.json` file content (lite path)
3. `submission/REFLECTION.md` (≤ 200 words): which anti-pattern from the “Top 5 Lakehouse Anti-Patterns” slide
   would your team's data be most at risk of, and why?
4. **Optional:** `submission/bonus/ARCHITECTURE.md` + `submission/bonus/poc/`
   for the [bonus challenge](BONUS-CHALLENGE.md).

## Late policy / regrade

Standard Track-2 policy applies — see `INDEX-Track2.md`.
