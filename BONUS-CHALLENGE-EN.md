# Bonus Challenge — Design Your Own Lakehouse

> 🇻🇳 Bản tiếng Việt: [`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md)

**Type:** Open-ended architecture brief — separate from the core lab; entirely optional.
**Audience:** You as the *architect on call*, not as a code-writer.
**Effort target:** 4–8 focused hours. Not a side hustle — a thoughtful afternoon.

The bonus exists for students who want to push past the rote deliverable and
build something they could actually defend in a senior design review. There's
no score attached. The reward is the work itself, the feedback you'll get on
your judgment, and a portfolio piece you can show to a hiring manager.

---

## The brief

Your team just inherited a hard data-storage problem. Pick one (or define your own
— same rules apply). Write **the architecture decision your team would defend in
a design review**. Code is optional; **the document is the deliverable**.

What we want to see is *judgment*: did you reject the obvious wrong answers for
the right reasons? "I considered Iceberg but went with Delta because *X*" is the
shape of an answer we want, not "Delta is good."

---

## Recommended topics (pick one — or invent yours)

Each is a real industrial problem. None has a single right answer.

### A. LLM observability at 1B requests/day
A foundation-model API team logs every request/response. **1B req/day, ~5 KB
each → 5 TB/day raw**. They need: (1) per-tenant cost & latency dashboards
refreshed every 5 min, (2) full prompt/response retained 7 days for incident
review, then aggregates only for 1 year, (3) PII redacted before any human can
read it, (4) total storage spend ≤ **\$5 K/mo**.
*Concepts to apply:* medallion layout, retention/lifecycle, FinOps tiering,
PII tokenization at Bronze, streaming ingestion, Z-order/clustering for the
"filter by tenant" hot path.

### B. Trillion-token training corpus with provenance
Like RedPajama — **30 TB of raw web crawl → 3 TB after MinHash-LSH dedup → 1 TB
curated training set**. Each shard carries a license tag (CC-BY, MIT,
proprietary, unknown). When a contamination report lands, you must (a) prove
which checkpoint trained on which shard, (b) revert that shard within 30 min,
(c) re-train downstream with the corrected corpus. Two regions: scrape in US,
train in EU.
*Concepts to apply:* Bronze→Silver→Gold curation, time travel + branching
(Nessie), catalog choice, multi-region replication semantics, training-data
provenance.

### C. Vietnamese ride-hailing CDC → Lakehouse (Decree 13 compliant)
Production Oracle DB → Debezium CDC → Lakehouse for analytics.
**100 M trips/year, 30 K writes/sec at peak.** Driver+rider PII (phone, ID,
GPS) in scope of **Decree 13/2023/NĐ-CP**. Analyst SLA: dashboards refreshed
within 60 s of source commit; ad-hoc queries p95 < 1 s. Late-arriving events
common (network drops in remote provinces).
*Concepts to apply:* CDC + Delta CDF, SCD Type 2, late-data handling
(`MERGE WHEN MATCHED AND src.ts > tgt.ts`), tokenization at Bronze landing,
audit table for every PII read, lineage tooling.

### D. Multimodal RAG over a 10 M-document legal corpus
A Vietnamese law firm wants RAG over **10 M PDFs** (text + scanned images +
tables). Embeddings will be regenerated **at least twice** as model upgrades
ship. Search latency p95 < 200 ms over 30 B tokens of chunks. Versioning
matters: when a court case cites a specific document version, the retrieval
result must be reproducible 5 years later.
*Concepts to apply:* Delta vs Lance for embeddings, embedding lifecycle &
versioning, multimodal storage layout, vector index choice (HNSW/IVF), RAG
freshness vs staleness.

### E. Hot/warm/cold lifecycle for click-stream under a hard FinOps cap
A consumer-app analytics team produces **10 TB/day of click events**.
Retention is regulatory: 365 days. The CFO's storage budget is a hard
**\$8 K/mo across all tiers**. Last-7-days queries must return p95 < 2 s;
last-90-days p95 < 30 s; > 90 days "best effort, < 5 min."
*Concepts to apply:* partitioning strategy, S3 Standard / IA / Glacier
tiering, compaction cadence, OPTIMIZE schedule cost trade-offs, query-time
projection / column pruning.

### F. Vendor-neutral catalog migration without downtime
You inherited 500 Delta tables in **Databricks Unity Catalog**, used by 20
teams across Spark, Trino, DuckDB, and Snowflake. Leadership wants out of
vendor lock-in: migrate to **Apache Polaris** with zero query downtime,
preserved time-travel semantics, and no broken dashboards. You have 6 weeks.
*Concepts to apply:* REST Catalog spec, Iceberg ↔ Delta interop (UniForm,
Apache XTable), table renames vs registration-only, dual-write windows,
governance migration.

### G. Real-time feature store with column-level lineage
A bank runs **200 features × 10 K production models**. The Risk team needs to
answer in < 5 min: *"If I drop column `customer.address_country`, which
models break and what's the dollar impact?"* Features are computed by ~50
Spark jobs across 5 teams, with daily backfills for the last 90 days.
*Concepts to apply:* OpenLineage + Marquez, data contracts, breaking-change
detection, feature-table layout, Bronze→Silver→Gold for features, MERGE-based
backfills.

### H. Your own topic
Define a problem from your own work or interest. Same rules: realistic scale
numbers, real constraints, real trade-offs.

---

## What to submit

A document, **3–6 pages** when rendered (markdown is fine), at
`submission/bonus/ARCHITECTURE.md` plus optional code at `submission/bonus/poc/`.

Use any structure you like, but the document must answer **all of these**:

1. **Problem statement** (≤ 200 words). Numbers, constraints, why hard.
2. **Architecture diagram.** Bronze→Silver→Gold layout, ingestion path, query
   path. ASCII art or any diagramming tool. Just *one* diagram, dense.
3. **Key decisions, with rejected alternatives.** For each major choice
   (table format, catalog, partitioning, compression, lifecycle, governance),
   write: *"I chose **A**. I rejected **B** because [tradeoff]. I rejected
   **C** because [tradeoff]."* Aim for ≥ 5 such decisions.
4. **Failure modes** (≥ 3). What goes wrong at 3 AM? How do you detect, and
   what's the rollback? Tie at least one to a Day 18 concept (time travel,
   schema evolution, deletion vectors, lineage).
5. **Cost back-of-envelope.** \$/month for storage + compute. Show the math.
   "$X/TB-month × Y TB" beats "should be cheap."
6. **What you would build first** (a one-week MVP slice). Not the whole thing
   — the smallest shippable cut that proves the architecture works.

**Optional PoC** (`submission/bonus/poc/`): a short notebook (50–150 lines)
that demos one *non-trivial* mechanism from your design — e.g., a tokenization
function for topic C, a license-tag-aware MERGE for topic B, an
embedding-version migration for topic D. Not a full implementation; a
spike that proves the hard part is feasible.

---

## What "great" looks like

The instructor will write a substantive review on your submission. Strong
submissions tend to share these traits — use them as a self-checklist before
you submit:

| Dimension | What strong work looks like |
|---|---|
| **Decisions with explicit alternatives** | At least 5 major choices each show ≥ 2 rejected options with concrete tradeoff reasoning. Generic *"X is faster"* ≠ a tradeoff. |
| **Realistic constraints & numbers** | Scale, latency, and budget figures show up *throughout* — not just in the problem statement. The math checks out. |
| **Connection to Day 18 concepts** | Medallion, ACID, time travel, catalogs, lineage, security, FinOps — at least 4 of these are *applied*, not just name-dropped. |
| **Failure-mode rigor** | Specific 3-AM scenarios with detection + rollback, not *"we'll monitor it."* |
| **Optional PoC quality** | If submitted: runs from a clean checkout; demonstrates the *hard* part, not the easy part. |

A generic submission with no tradeoff reasoning gets a generic review. The
work is the reward; the depth you put in is the depth you get back.

---

## How to start (if you're stuck)

1. Pick a topic. Spend **20 min** sketching what already exists in your head —
   diagram, formats, partitions, catalog. Don't research yet.
2. List **5 things that could break** the design at 3 AM. For each, write the
   rollback plan in one sentence.
3. *Now* go look at the slide deck §6–§8 (Industrial cases, Production ops)
   and notice which trade-offs you forgot.
4. Revise. Write. Submit.

The first sketch is always wrong. The second one is the work.

---

## Submission

Push to your fork at `submission/bonus/ARCHITECTURE.md`. Same PR as the core
lab — title prefix the bonus: `[NXX] Lab18 — <Họ Tên> [+bonus]`.
