#!/usr/bin/env python3
"""Debug: Where is Kontra spending time?"""

import os
import time

os.environ["AWS_ACCESS_KEY_ID"] = "saevarl"
os.environ["AWS_SECRET_ACCESS_KEY"] = "abc12345"
os.environ["AWS_ENDPOINT_URL"] = "http://100.111.146.121:9000"
os.environ["AWS_REGION"] = "us-east-1"

S3_FILE = "s3://test/data/users_5m.parquet"

import kontra
from kontra import rules

print("Testing each rule individually on S3 (5M rows):\n")

test_rules = [
    ("not_null", [rules.not_null("user_id")]),
    ("unique", [rules.unique("user_id")]),
    ("range", [rules.range("age", min=0, max=120)]),
    ("allowed_values", [rules.allowed_values("status", ["active", "inactive", "pending"])]),
]

for name, rule_list in test_rules:
    start = time.time()
    result = kontra.validate(S3_FILE, rules=rule_list, save=False)
    elapsed = time.time() - start
    status = "✓" if result.passed else "✗"
    print(f"{name:20} {elapsed:>6.2f}s {status}")

print("\n--- All rules together ---")
start = time.time()
result = kontra.validate(S3_FILE, rules=[
    rules.not_null("user_id"),
    rules.unique("user_id"),
    rules.range("age", min=0, max=120),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
], save=False)
elapsed = time.time() - start
print(f"{'combined':20} {elapsed:>6.2f}s")

print("\n--- Without unique ---")
start = time.time()
result = kontra.validate(S3_FILE, rules=[
    rules.not_null("user_id"),
    rules.range("age", min=0, max=120),
    rules.allowed_values("status", ["active", "inactive", "pending"]),
], save=False)
elapsed = time.time() - start
print(f"{'no unique':20} {elapsed:>6.2f}s")
