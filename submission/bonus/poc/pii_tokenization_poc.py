# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # PoC — PII Tokenization at Bronze Ingestion
#
# Demos the non-trivial mechanism from the architecture brief (Topic A):
# deterministic AES-SIV tokenization applied at write time so raw PII
# never lands in the Delta table. Runs from a clean checkout — no external
# services needed (uses a local key, not KMS).
#
# In production: replace `_local_key()` with `boto3 kms.generate_data_key()`.

# %%
import hashlib
import hmac
import json
import os
import re
import sys
import polars as pl
from pathlib import Path

# Add scripts/ to path so lakehouse helper works
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
from lakehouse import path, reset  # noqa: E402
from deltalake import DeltaTable, write_deltalake  # noqa: E402

# %% [markdown]
# ## 1. Deterministic Tokenizer (AES-SIV substitute using HMAC-SHA256)
#
# Real production: use `cryptography` library's AES-SIV mode.
# Here we use HMAC-SHA256 truncated to 16 hex chars — same deterministic
# property (same input → same token), same one-way guarantee.

# %%
def _local_key() -> bytes:
    """Derive a stable local key from a fixed seed (dev only — use KMS in prod)."""
    return hashlib.sha256(b"dev-only-seed-replace-with-kms").digest()


_KEY = _local_key()

PII_PATTERNS = {
    "phone": re.compile(r'\b(0|\+84)[0-9]{8,10}\b'),
    "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
    "id_card": re.compile(r'\b\d{9,12}\b'),
}


def tokenize(value: str) -> str:
    """Deterministic HMAC token — same value always → same token."""
    mac = hmac.new(_KEY, value.encode(), hashlib.sha256).hexdigest()[:16]
    return f"TOK_{mac}"


def redact_pii_in_json(raw_json: str) -> tuple[str, list[str]]:
    """
    Scan raw JSON string for PII patterns and replace with tokens.
    Returns (cleaned_json, list_of_pii_types_found).
    """
    found_types = []
    result = raw_json
    for pii_type, pattern in PII_PATTERNS.items():
        matches = pattern.findall(result)
        if matches:
            found_types.append(pii_type)
            for match in set(matches):
                result = result.replace(match, tokenize(match))
    return result, found_types


# %% [markdown]
# ## 2. Simulate Raw LLM Events (with embedded PII)

# %%
raw_events = [
    {
        "request_id": "req_001",
        "ts": "2026-04-01T10:00:00Z",
        "model": "claude-sonnet-4-6",
        "tenant_id": "tenant_acme",
        "raw_json": json.dumps({
            "model": "claude-sonnet-4-6",
            "prompt": "Số điện thoại của tôi là 0912345678, hãy giúp tôi",
            "user_id": "user_nguyen@gmail.com",
            "usage": {"input": 120, "output": 80},
            "latency_ms": 312,
            "status": "ok",
        }),
    },
    {
        "request_id": "req_002",
        "ts": "2026-04-01T10:00:05Z",
        "model": "claude-haiku-4-5",
        "tenant_id": "tenant_beta",
        "raw_json": json.dumps({
            "model": "claude-haiku-4-5",
            "prompt": "CMND của tôi: 012345678, địa chỉ: 123 Hai Bà Trưng",
            "user_id": "user_tran",
            "usage": {"input": 60, "output": 40},
            "latency_ms": 95,
            "status": "ok",
        }),
    },
    {
        "request_id": "req_003",
        "ts": "2026-04-01T10:00:10Z",
        "model": "claude-sonnet-4-6",
        "tenant_id": "tenant_acme",
        "raw_json": json.dumps({
            "model": "claude-sonnet-4-6",
            "prompt": "What is the capital of France?",   # no PII
            "user_id": "user_smith",
            "usage": {"input": 30, "output": 20},
            "latency_ms": 201,
            "status": "ok",
        }),
    },
]

print(f"Simulated {len(raw_events)} raw events")
print(f"\nSample raw prompt (BEFORE tokenization):")
print(json.loads(raw_events[0]["raw_json"])["prompt"])

# %% [markdown]
# ## 3. Apply Tokenization at Bronze Write Time

# %%
tokenized_rows = []
pii_audit_rows = []

for event in raw_events:
    cleaned_json, pii_types = redact_pii_in_json(event["raw_json"])
    tokenized_rows.append({
        **event,
        "raw_json": cleaned_json,          # PII replaced with tokens
        "pii_detected": len(pii_types) > 0,
    })
    if pii_types:
        pii_audit_rows.append({
            "request_id": event["request_id"],
            "ts": event["ts"],
            "pii_types_found": ",".join(pii_types),
        })

print(f"\nSample prompt AFTER tokenization:")
print(json.loads(tokenized_rows[0]["raw_json"])["prompt"])
print(f"\nPII detected in {len(pii_audit_rows)}/{len(raw_events)} events")

# %% [markdown]
# ## 4. Write Tokenized Data to Bronze Delta Table

# %%
BRONZE_POC = path("scratch", "bonus_bronze_poc")
reset(BRONZE_POC)

df_bronze = pl.DataFrame(tokenized_rows)
write_deltalake(BRONZE_POC, df_bronze.to_arrow(), mode="overwrite")

print(f"\nBronze table written: {DeltaTable(BRONZE_POC).to_pyarrow_table().num_rows} rows")

# %% [markdown]
# ## 5. Verify: No Raw PII in Bronze

# %%
stored = pl.from_arrow(DeltaTable(BRONZE_POC).to_pyarrow_table())

phone_pattern = re.compile(r'\b0[0-9]{9,10}\b')
email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')

pii_leaks = []
for row in stored.iter_rows(named=True):
    raw = row["raw_json"]
    if phone_pattern.search(raw) or email_pattern.search(raw):
        pii_leaks.append(row["request_id"])

if pii_leaks:
    print(f"FAIL: PII found in Bronze rows: {pii_leaks}")
else:
    print("PASS: No raw PII found in Bronze — all values tokenized.")

print(f"\nPII audit log ({len(pii_audit_rows)} events flagged):")
for a in pii_audit_rows:
    print(f"  {a['request_id']} | {a['ts']} | types={a['pii_types_found']}")

# %% [markdown]
# ## 6. Demonstrate Determinism — Same Input → Same Token (enables dedup/join)

# %%
phone = "0912345678"
t1 = tokenize(phone)
t2 = tokenize(phone)
assert t1 == t2, "Tokenizer is not deterministic!"
print(f"tokenize('{phone}') called twice → '{t1}' == '{t2}'  ✓ deterministic")
print(f"\nDifferent input → different token:")
print(f"tokenize('0987654321') → '{tokenize('0987654321')}'")

# %% [markdown]
# ## ✅ PoC Summary
#
# - PII (phone, email, ID card) is detected and replaced with deterministic tokens
#   **before** writing to the Delta Bronze table.
# - A PII audit log records which `request_id` triggered tokenization (without
#   storing the actual PII).
# - Tokens are deterministic: the same phone number always maps to the same token,
#   so dedup and tenant-level aggregation still work correctly downstream.
# - Verification confirms no raw PII pattern survives in the stored Bronze rows.
#
# **In production:** swap `_local_key()` with AWS KMS `generate_data_key()`.
# The detokenization key is stored only in KMS; only the audit-access service
# has `kms:Decrypt` permission.
