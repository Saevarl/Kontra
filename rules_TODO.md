1) New rules (with pushdown where sensible)
1.1 allowed_values

Goal: Validate a column is within a small enum; SQL pushdown for small sets.

Do this

 Spec: params: { column, values: [..] }; optional case_sensitive: bool=false.

 Implement Polars path: ~pl.col(col).is_in(values).

 Implement SQL path (DuckDB): WHERE col NOT IN (...).

 Add guard: only push down if len(values) <= ENUM_PUSH_CAP (e.g., 100).

 Rule ID: COL:<col>:allowed_values.

 Message parity: “<col> has N value(s) outside allowed set”.

 Register in RuleRegistry; add to planner’s SQL-capable list.

 Projection: mark required_cols = [col].

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: allowed_values
    params: { column: status, values: ["active","inactive","pending"] }


Tests

 Unit: values inside/outside; case sensitivity option.

 Integration: pushdown=auto vs off → identical results; projection effective.

 Large enum (> cap) stays in Polars; verify planner chose residual.

1.2 in_range

Goal: Numeric range check with inclusive/Exclusive bounds.

Do this

 Spec: params: { column, min, max, inclusive: true }.

 Polars: is_between(min,max, closed="both"|"left"|"right"|"none").

 SQL: WHERE col < min OR col > max (adjust for exclusive).

 Rule ID: COL:<col>:in_range.

 Message: “<col> outside [min,max] (inclusive)”.

 Planner wiring + projection.

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: in_range
    params: { column: age, min: 13, max: 95, inclusive: true }


Tests

 Boundary values pass/fail per inclusivity.

 Parity auto/off.

 Mixed types (ints/floats) behave sensibly.

1.3 unique (SQL pushdown v1.1)

Goal: Uniqueness per column.

Do this

 Polars: is_duplicated().any() + count duplicates.

 SQL: GROUP BY col HAVING COUNT(*)>1 (return violators sample).

 Rule ID: COL:<col>:unique.

 Message: “<col> has N duplicate(s)”.

 Planner pushdown; projection requires [col].

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: unique
    params: { column: user_id }


Tests

 Dataset with injected duplicates.

 Parity auto/off; SQL wins on overlap.

 Large cardinality performance (10M rows sample).

1.4 freshness

Goal: Ensure latest timestamp not older than a threshold.

Do this

 Spec: params: { column, max_age: "30d" } (support “h/m/d”).

 Polars: df[col].max() >= now - timedelta.

 SQL: SELECT MAX(col) < now()-INTERVAL ... → fail when true.

 Rule ID: COL:<col>:freshness.

 Message: “<col> max ts is older than 30d”.

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: freshness
    params: { column: last_login, max_age: "30d" }


Tests

 Control clock by generating data around threshold.

 Parity auto/off.

1.5 percent_null

Goal: Nulls under a threshold.

Do this

 Spec: params: { column, lte: 0.02 }.

 Polars: df[col].is_null().mean() <= p.

 SQL: SUM(col IS NULL)::DOUBLE / COUNT(*) <= p.

 Rule ID: COL:<col>:percent_null.

 Message: “<col> null ratio p>N%”.

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: percent_null
    params: { column: email, lte: 0.02 }


Tests

 Controlled null injection (0%, 1%, 5%).

 Parity auto/off.

2) Custom logic routes
2.1 custom_sql_check (finalize)

Goal: Fail if the provided SQL returns any rows.

Do this

 Confirm placeholder: ${DATASET} → bound table/view name in DuckDB.

 Support sample_limit to cap returned violators.

 Message: “custom_sql_check returned N row(s)”.

 Make sure planner requires zero extra columns (unless requested).

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: custom_sql_check
    params:
      sql: "SELECT user_id FROM read_parquet('${DATASET}') WHERE balance < 0"
      sample_limit: 50


Tests

 Generate negatives; assert failure and count >0.

 Bad SQL → executor error → engine handles and surfaces a clear message.

2.2 python_udf (safe, import-path based)

Goal: Allow project-defined validation without inline code.

Do this

 Spec: params: { fn: "pkg.mod.func", columns: [...], config: {...} }.

 Load via importlib; call fn(df, config=...).

 Normalize return to {passed, failed_count, message}.

 Rule ID: UDF:<fn> or COL:<col>:python_udf if single-col.

 Non-pushdown; projection uses columns.

Manual contract

dataset: "tests/data/users.parquet"
rules:
  - name: python_udf
    params:
      fn: "my_project.qc_rules.must_be_adult"
      columns: ["age","is_premium"]
      config: { min_age: 18 }


Tests

 Tiny UDF module in tests; deterministic fail/pass.

 Errors in UDF → rule fails gracefully with message, engine doesn’t crash.

3) Planner/engine hardening (light but valuable)
3.1 Pushdown capability flags

Do this

 Each rule advertises: supports_sql: bool, semantics: "strict"|"approx".

 Planner only pushes if supports_sql and semantics == "strict" or user opts into approx.

 Add global flag/env: KONTRA_PUSH_APPROX=1 to allow approx pushdown.

Tests

 Force approx off: rule stays residual.

 Force approx on: rule goes SQL; parity holds for your datasets.

3.2 Violation sampling

Do this

 Standardize sample_limit param for rules that can return violators (SQL and Polars).

 Ensure Polars path also respects sample_limit to keep output sizes sane.

Tests

 With many failures, output includes ≤ sample_limit exemplars.

4) Test suite additions
4.1 Parity matrix tests (auto vs off)

Do this

 For each new pushdown rule, add a pytest.mark.parametrize("pushdown", ["auto","off"]).

 Assert rules_passed/failed and failed_count identical.

 Assert projection stats behave (loaded ≤ available; equals required when off).

4.2 Boundary/value-case tests

Do this

 Min/max boundary inclusivity (in_range).

 Case sensitivity for allowed_values (if added).

 Date/time boundaries for freshness.

4.3 Negative-path tests

Do this

 Bad SQL syntax in custom_sql_check → executor error surfaced, engine survives.

 UDF raises → rule reports error, engine survives.

 Executor boom → fallback to Polars (you already have a pattern; reuse).

4.4 Determinism extension

Do this

 Add tests that sort results by rule_id before comparing to eliminate ordering noise.

 Verify rule_id stability when rule order changes in YAML (optional, if you want order-independent IDs).

5) Docs & DX
5.1 Rule reference docs

Do this

 For each rule: params, examples, SQL pushdown notes, edge cases.

 Indicate whether rule is SQL-capable and any size caps (e.g., enum size).

5.2 Contracts gallery

Do this

 Add a contracts/examples/ folder with the manual contracts above.

 Include a README with “Try this” commands:

contra validate contracts/examples/allowed_values.yml --output-format rich
contra validate contracts/examples/custom_sql.yml --no-actions

5.3 CLI help polish

Do this

 Add contra plan (even minimal) to print compiled plan: required cols, sql rules, residual rules.

6) “Done” definition (per feature)

For every new/modified rule or engine behavior:

 Spec settled (params documented).

 Polars path implemented with tests.

 SQL path implemented (if applicable) with tests.

 Planner wired (projection, capability flags).

 Rule IDs deterministic and messages consistent.

 Parity auto/off proven.

 Manual contract runs cleanly.

 Docs updated (rule reference + example).

 Bench sanity (quick run on 10M rows if performance-sensitive).

quick starter order (low effort → high impact)

allowed_values, in_range, percent_null (fast, clear, pushdownable).

freshness (handy + pushdownable).

unique pushdown (slightly more work, big win).

custom_sql_check hardening (placeholders + sampling).

python_udf rule (DX unlock; Polars-only).

If you want, I can stub the code skeletons for rules 1–3 in Kontra style so you can drop them straight into kontra/rules/builtin/ and start wiring tests.