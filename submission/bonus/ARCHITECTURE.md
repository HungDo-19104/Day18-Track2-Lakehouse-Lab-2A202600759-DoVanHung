# Architecture Brief — LLM Observability Lakehouse at 1B Requests/Day

**Topic:** A — LLM observability ở quy mô 1B requests/ngày
**Author:** Đỗ Văn Hưng — AICB-P2T2 Day 18

---

## 1. Problem Statement

Một foundation-model API team log mọi request/response từ 1B lượt gọi mỗi ngày.
Với ~5 KB/request, hệ thống sinh ra **5 TB raw data mỗi ngày**. Yêu cầu cứng:

- **Dashboard**: cost & latency theo từng tenant, refresh mỗi 5 phút — tức là
  Silver/Gold phải sẵn sàng trong vòng dưới 5 phút từ lúc request xảy ra.
- **Audit trail**: giữ prompt/response đầy đủ 7 ngày để review incident; sau đó
  chỉ giữ aggregates trong 1 năm.
- **PII**: mọi prompt/response có thể chứa dữ liệu nhạy cảm. Phải được redact
  hoặc tokenize **trước khi bất kỳ người nào đọc được**.
- **Budget**: tổng chi phí storage ≤ **$5,000/tháng**.

Điều khiến bài này khó: throughput ghi (5 TB/ngày ≈ 60 MB/giây liên tục) xung
đột với yêu cầu đọc latency thấp; PII phải xử lý tại điểm ingestion chứ không
thể để downstream; budget cứng buộc phải tính toán lifecycle chi li.

---

## 2. Architecture Diagram

```
                        ┌─────────────────────────────────────────┐
  LLM API Servers       │             INGESTION LAYER              │
  (1B req/day)          │                                          │
  ──────────────►  Kafka Topic: llm-raw-events                    │
  ~60 MB/s peak  │      │  partitions=200, retention=24h           │
                 │      └─────────────────┬───────────────────────┘
                 │                        │
                 │              Spark Structured Streaming
                 │              (micro-batch, 30s trigger)
                 │                        │
                 │                        ▼
                 │      ┌─────────────────────────────────────────┐
                 │      │  BRONZE — s3://lakehouse/bronze/         │
                 │      │  llm_calls_raw/                          │
                 │      │  • Raw JSON + metadata headers           │
                 │      │  • PII tokenized at write time           │◄── tokenize() UDF
                 │      │  • Partition: date= / hour=              │    runs here
                 │      │  • Format: Delta Lake (snappy)           │
                 │      │  • Retention: 7 ngày → VACUUM            │
                 │      └──────────────┬──────────────────────────┘
                 │                     │
                 │           Delta CDF (Change Data Feed)
                 │           Spark batch, trigger mỗi 2 phút
                 │                     │
                 │                     ▼
                 │      ┌─────────────────────────────────────────┐
                 │      │  SILVER — s3://lakehouse/silver/         │
                 │      │  llm_calls/                              │
                 │      │  • Dedup by request_id (MERGE)           │
                 │      │  • Typed columns, validated              │
                 │      │  • Partition: date= / tenant_id=         │
                 │      │  • Z-ORDER: (tenant_id, model)           │
                 │      │  • Format: Delta Lake (zstd)             │
                 │      │  • Retention: 90 ngày                    │
                 │      └──────────────┬──────────────────────────┘
                 │                     │
                 │           DuckDB / Spark batch
                 │           trigger mỗi 2 phút (Gold hot)
                 │           + mỗi 1 giờ (Gold cold)
                 │                     │
                 │                     ▼
                 │      ┌─────────────────────────────────────────┐
                 │      │  GOLD — s3://lakehouse/gold/             │
                 │      │  llm_daily_metrics/  (daily agg)         │
                 │      │  llm_5min_metrics/   (5-min agg)         │
                 │      │  • p50/p95 latency, cost_usd, error_rate │
                 │      │  • GROUP BY (window, tenant_id, model)   │
                 │      │  • Format: Delta Lake (zstd)             │
                 │      │  • Retention: 1 năm                      │
                 │      └──────────────┬──────────────────────────┘
                 │                     │
                 │            Query Layer
                 │     ┌───────────────┴───────────────┐
                 │     │                               │
                 │  Grafana (5-min Gold)          Ad-hoc (DuckDB
                 │  p95 < 2s dashboard            trực tiếp Silver)
                 │                                p95 < 1s
                 │
                 └──► Audit Log Table (mọi PII access được ghi lại)
```

---

## 3. Quyết Định Chính — Kèm Alternatives Đã Loại

### D1: Table Format — Delta Lake

**Tôi chọn Delta Lake.**

- Tôi loại **Apache Iceberg** vì: tại thời điểm này delta-rs (Rust binding) cho
  phép writer không cần JVM, giảm đáng kể overhead khi chạy trên các edge
  ingestion node nhỏ. Iceberg có catalog dependency (Hive, Glue, Nessie) thêm
  một điểm failure; Delta với file-based `_delta_log` self-contained hơn.
- Tôi loại **raw Parquet** vì: không có ACID — với 60 MB/s concurrent writer,
  partial write failure sẽ gây corruption không phát hiện được. Ngoài ra
  không có time travel để rollback khi PII tokenizer bug ra production.
- Tôi loại **Apache Hudi** vì: MoR (Merge-on-Read) của Hudi phù hợp CDC
  workload, nhưng bài này là append-heavy; CoW (Copy-on-Write) của Delta
  đơn giản hơn và compaction predictable hơn ở throughput này.

### D2: Partitioning Strategy — date= / hour= ở Bronze, date= / tenant_id= ở Silver

**Tôi chọn partition Bronze theo `date` + `hour`.**

- Tôi loại **partition theo `tenant_id`** ở Bronze vì: số tenant có thể lên
  đến hàng nghìn, tạo ra small-file explosion (~1000 tenant × 24h = 24,000
  partitions/ngày). OPTIMIZE sẽ phải chạy liên tục và tốn kém.
- Tôi loại **không partition** vì: VACUUM 7-ngày sẽ phải scan toàn bộ Bronze
  thay vì drop nguyên một partition folder — với 35 TB, điều này không chấp nhận
  được về chi phí S3 API calls.
- Tôi chọn **Silver partition theo `tenant_id`** vì đây là filter hot nhất
  trong query path của dashboard (Grafana luôn filter theo tenant), nên file
  pruning tại Silver trực tiếp giảm bytes scanned.

### D3: PII Strategy — Tokenization tại Bronze, không phải Redaction tại Silver

**Tôi chọn tokenize tại điểm ghi Bronze.**

- Tôi loại **redact tại Silver** vì: khoảng thời gian từ lúc raw đến lúc
  Silver chạy (~2 phút) là cửa sổ rủi ro — nếu có data breach hoặc engineer
  access Bronze bằng tay, raw PII đã lộ. Với Decree 13 / GDPR tương đương,
  window này không chấp nhận được.
- Tôi loại **encrypt at rest chỉ** vì: mã hóa disk không ngăn được query-time
  exposure — một SELECT * từ Spark vẫn trả về PII plaintext cho người có
  IAM access.
- Tokenization bằng AES-SIV với key trong AWS KMS: `tokenize(phone)` → deterministic
  token cho phép dedup và join mà không expose PII. Token có thể được detokenize
  bởi hệ thống có quyền riêng (audit-only).

### D4: Ingestion — Kafka + Spark Structured Streaming

**Tôi chọn Kafka → Spark Structured Streaming với micro-batch 30 giây.**

- Tôi loại **Kinesis Firehose → S3 → batch** vì: Firehose buffer tối thiểu 60
  giây, và batch size 128 MB mặc định tạo ra many small files tại S3 trước khi
  Delta writer consolidate. Overhead double-write (Firehose → S3 raw → Delta)
  tốn thêm ~$400/tháng PUT requests.
- Tôi loại **Flink** vì: Flink tốt hơn cho stateful streaming (CEP, windowed
  join), nhưng workload này là stateless append + simple dedup. Spark có Delta
  writer native hơn; Flink cần delta-flink connector thêm dependency.
- Micro-batch 30 giây (không phải continuous) vì: 30s latency đủ cho dashboard
  5-phút; continuous streaming tạo ra quá nhiều small Delta commits (~120/giờ)
  làm `_delta_log` phình to, tăng planning time.

### D5: Retention & Lifecycle — Delta VACUUM + S3 Lifecycle Rules

**Tôi chọn kết hợp Delta VACUUM cho Bronze (7 ngày) và S3 Lifecycle cho
Silver/Gold cold tier.**

- Tôi loại **chỉ dùng S3 Lifecycle** mà không VACUUM vì: S3 Lifecycle xóa
  object file nhưng Delta `_delta_log` vẫn tham chiếu đến file đó → đọc bị
  broken, `DeltaTable` sẽ throw `FileNotFoundException`. Phải VACUUM trước để
  Delta dọn reference, sau đó S3 Lifecycle mới được phép xóa.
- Tôi loại **giữ Bronze mãi trên S3 Standard** vì: 35 TB × $0.023 = $805/tháng
  chỉ riêng Bronze storage, chưa tính compute. Sau 7 ngày, data không còn cần
  thiết cho incident review.
- Silver sau 90 ngày: chuyển sang S3 Infrequent Access ($0.0125/GB). Gold giữ
  1 năm trên Standard vì nhỏ (~20 GB aggregates, $0.46/tháng).

### D6: Catalog — AWS Glue Data Catalog (managed)

**Tôi chọn AWS Glue Data Catalog.**

- Tôi loại **Databricks Unity Catalog** vì: vendor lock-in — nếu compute layer
  sau này chuyển từ Databricks sang EMR hoặc EKS, catalog migration sẽ tốn
  sprint dài.
- Tôi loại **Apache Hive Metastore tự quản** vì: thêm một service cần HA, backup,
  upgrade cycle. Ở throughput này một HMS outage sẽ block toàn bộ ingestion.
- Glue là managed, tích hợp native với S3 + Athena + EMR, và Delta Lake hỗ trợ
  Glue catalog qua `delta.catalog.s3` config.

---

## 4. Failure Modes — 3 AM Scenarios

### FM1: PII Tokenizer Bug — Raw PII Leak vào Bronze

**Tình huống:** Một deploy mới của tokenize UDF có bug — `tokenize()` trả về
input gốc thay vì token (silent failure, không exception).

**Detection:** Monitoring job chạy mỗi 10 phút sample 100 rows từ Bronze mới
nhất, check bằng regex `r'\b\d{10,11}\b'` (phone pattern) và email pattern.
Nếu match rate > 0 → alert PagerDuty ngay lập tức.

**Rollback:** Delta time travel là chìa khóa.
```python
# Identify last clean version
dt = DeltaTable(BRONZE)
# Rollback to version trước deploy
dt.restore(version=last_clean_version)
```
Sau đó re-process từ Kafka (retention 24h) với tokenizer đã fix. Window rủi ro:
tối đa 10 phút (interval monitor) × volume ~36 GB — scope có thể xác định chính
xác qua `_delta_log` timestamp.

### FM2: Small-File Explosion — OPTIMIZE Job Fail Giữa Chừng

**Tình huống:** OPTIMIZE job (chạy mỗi 2 giờ trên Silver) bị OOM killed sau khi
đã commit một phần → Silver có mix của file nhỏ chưa compact và file đã compact,
dẫn đến planning overhead tăng dần, query latency leo thang.

**Detection:** CloudWatch metric `silver_file_count` > threshold (ví dụ > 10,000
files/partition) → alert. DuckDB query `SELECT count(*) FROM delta_scan(SILVER)
SHOW FILES` trong smoke test hàng giờ.

**Rollback:** Không cần rollback data — OPTIMIZE là idempotent, không thay đổi
rows. Chạy lại OPTIMIZE với cluster lớn hơn:
```python
DeltaTable(SILVER).optimize.compact()  # re-compact chỉ affected partitions
```
Delta đảm bảo query vẫn chạy đúng trong thời gian này (chỉ chậm hơn).

### FM3: Schema Evolution Không Tương Thích — LLM Provider Thay Đổi Response Format

**Tình huống:** OpenAI thay đổi response schema (`usage.input_tokens` → đổi tên
thành `usage.prompt_tokens`), Spark Streaming writer bắt đầu fail với
`AnalysisException: Column 'usage.input' not found`.

**Detection:** Streaming job health metric: nếu `records_written` drop xuống 0
trong 2 consecutive micro-batch → alert. `_delta_log` sẽ không có commit mới.

**Rollback / Fix:**
1. Pause streaming job, không mất data vì Kafka còn giữ 24h.
2. Update Bronze schema với `schema_mode="merge"` để thêm column mới song song
   với column cũ — backward compatible.
3. Cập nhật Silver transformation để handle cả hai column name.
4. Resume từ Kafka offset trước khi schema break.

Delta schema evolution (bài học từ NB1) cho phép `tier`-style merge mà không
drop existing data — áp dụng y hệt ở đây.

---

## 5. Ước Lượng Chi Phí — Back-of-Envelope

### Storage

| Layer | Size | Tier | $/GB-month | $/month |
|---|---|---|---|---|
| Bronze (7 ngày × 5 TB/ngày raw, 5:1 compress) | 7 TB | S3 Standard | $0.023 | **$161** |
| Silver (90 ngày aggregated, ~10:1 compress) | 45 TB | S3 Standard → IA | $0.0125 avg | **$563** |
| Gold (365 ngày, daily metrics ~50 MB/ngày) | 18 GB | S3 Standard | $0.023 | **$0.4** |
| Kafka (200 partitions × 24h retention) | ~120 GB | MSK r5.2xlarge ×3 | flat | **$800** |
| **Storage subtotal** | | | | **~$1,525** |

### Compute

| Component | Spec | $/month |
|---|---|---|
| Spark Streaming (ingestion + Bronze write) | 4 × r5.xlarge (16 vCPU, 128 GB) | $700 |
| Spark batch Silver refresh (mỗi 2 phút) | 2 × r5.xlarge spot | $180 |
| Gold aggregation (DuckDB, serverless) | Lambda / Fargate | $50 |
| OPTIMIZE jobs (2h cadence) | r5.2xlarge spot, 30 min/run | $120 |
| **Compute subtotal** | | **~$1,050** |

### S3 API Costs

- 5 TB/ngày × 30 = 150 TB/tháng PUT → ~1B PUT requests → $5,000... **quá cao**.
- Fix: batch writes thành files ≥ 128 MB (Spark default), giảm số PUT xuống
  ~1M/tháng → $5/tháng.

### **Tổng: ~$2,580/tháng — dưới budget $5,000/tháng.**

Buffer ~$2,400 dùng cho: data transfer, CloudWatch, Glue catalog calls, và
spike traffic (ví dụ viral moment × 3 throughput).

---

## 6. MVP Một Tuần — Smallest Shippable Slice

**Mục tiêu:** Chứng minh end-to-end pipeline từ raw event → Gold metric trong
môi trường staging với 1% traffic thật (~10M req/ngày).

**Tuần 1 — chỉ làm những thứ này:**

1. **Ngày 1–2:** Kafka topic setup (20 partitions cho staging) + Spark Streaming
   job ghi Bronze với tokenizer. Verify PII monitor alert bắn đúng.

2. **Ngày 3:** Silver transformation: dedup + typed columns. Chạy batch thủ
   công, verify `Silver rows < Bronze rows`.

3. **Ngày 4:** Gold 5-phút aggregation. Kết nối Grafana vào Gold, xác nhận
   dashboard hiển thị data trong vòng < 5 phút từ khi event vào Kafka.

4. **Ngày 5:** VACUUM job cho Bronze (7-ngày), chạy lần đầu, verify Delta
   history còn đủ để time-travel nhưng disk giảm.

**Không làm trong tuần 1:** multi-tenant isolation, S3 lifecycle rules, OPTIMIZE
automation, HA cho Kafka. Những thứ này scale-out sau khi core pipeline đã
proven.

**Pass criterion:** Cuối tuần 1, một incident drill: inject 1,000 rows với
fake phone number vào Bronze, verify monitor alert trong < 10 phút, rollback
bằng `dt.restore()`, verify PII không còn trong Silver.

---

## Liên Hệ Concepts Day 18

| Concept | Áp dụng ở đâu |
|---|---|
| **Medallion Architecture** | Bronze (raw+tokenized) → Silver (dedup+typed) → Gold (5-min + daily agg) |
| **ACID / Delta transaction log** | Concurrent Spark writer không corrupt Bronze; FM3 rollback dùng `_delta_log` |
| **Time Travel** | FM1 rollback PII leak dùng `dt.restore(version=N)`; audit: "data tại thời điểm incident trông như thế nào?" |
| **Schema Evolution** | FM3: `schema_mode="merge"` khi LLM provider đổi response format |
| **Z-ORDER** | Silver Z-ORDER trên `(tenant_id, model)` — filter hot path của Grafana dashboard |
| **FinOps / Lifecycle** | VACUUM Bronze 7 ngày; S3-IA cho Silver > 30 ngày; math kiểm tra được tổng < $5K |
| **Deletion Vectors** | Khi có GDPR right-to-erasure request: soft-delete bằng deletion vectors thay vì rewrite toàn partition |
| **Lineage** | OpenLineage emit từ mọi job → Marquez: trace từ Gold metric → Silver row → Bronze raw → Kafka offset |
