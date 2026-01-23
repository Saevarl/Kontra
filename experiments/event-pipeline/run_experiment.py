"""
Agent Transformation Experiment: Event Pipeline (Multi-Step)

Tests whether Kontra's compare() probe helps agents debug multi-step pipelines.

This is designed to require compare():
- 4 sequential transformation steps
- Errors compound across steps
- Final validation failure doesn't indicate which step failed
- compare() after each step catches issues early

Usage:
    python run_experiment.py --condition control    # No probes
    python run_experiment.py --condition treatment  # With probes
    python run_experiment.py --both                 # Run both and compare
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI

# =============================================================================
# Configuration
# =============================================================================

MAX_ITERATIONS = 25  # More iterations for complex multi-step task
MODEL = "gpt-4o"

EXPERIMENT_DIR = Path(__file__).parent
DATA_DIR = EXPERIMENT_DIR / "data"

# =============================================================================
# Tool definitions
# =============================================================================

TOOL_EXECUTE_PYTHON = {
    "type": "function",
    "function": {
        "name": "execute_python",
        "description": "Execute Python code and return the output. Available libraries: polars (as pl), kontra.",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python code to execute"
                }
            },
            "required": ["code"]
        }
    }
}

TOOL_DECLARE_SUCCESS = {
    "type": "function",
    "function": {
        "name": "declare_success",
        "description": "Call this when validation passes and you're satisfied with the solution.",
        "parameters": {
            "type": "object",
            "properties": {
                "explanation": {
                    "type": "string",
                    "description": "Brief explanation of the solution"
                }
            },
            "required": ["explanation"]
        }
    }
}

# =============================================================================
# System prompts
# =============================================================================

SYSTEM_PROMPT_BASE = """You are a data engineer solving a multi-step data transformation pipeline.

## Task
Create a pipeline that produces `daily_user_activity` from raw event logs:

**Step 1: Deduplicate** - Remove duplicate events (same event_id, keep latest timestamp)
**Step 2: Enrich** - Join with users table to get user_name and user_tier
**Step 3: Filter** - Keep only completed events (watch for status casing issues!)
**Step 4: Aggregate** - Group by user_id + event_date, calculate counts and sums

## Target Schema
- `user_date_key`: Composite key "{{user_id}}_{{event_date}}"
- `user_id`: User identifier
- `user_name`: From users table
- `user_tier`: From users table
- `event_date`: Date in YYYY-MM-DD format
- `completed_events`: Count of completed events
- `total_amount`: Sum of amounts

## Data
- `data/events.parquet` - Raw event logs (event_id, user_id, event_type, status, event_date, event_timestamp, amount)
  - NOTE: `event_date` is already extracted as YYYY-MM-DD string
- `data/users.parquet` - User lookup (user_id, user_name, user_tier, signup_date)

## Success Criteria
```python
result = kontra.validate(output_df, "target_contract.yml")
# result.passed must be True
```

## Available Libraries (in execute_python tool)
- polars (imported as pl) - NOTE: Use `df.group_by()` not `df.groupby()`
- kontra

{probe_docs}

## Important
- This is a MULTI-STEP pipeline - errors compound!
- Think step by step
- After each code execution, analyze the results before writing more code
- Use print() to see outputs
- When validation passes (result.passed == True), call declare_success()
"""

PROBE_DOCS_NONE = """## Kontra Functions
- `kontra.validate(df, contract_path)` - Validate data against a contract. Returns result with .passed (bool) and .blocking_failures (list)
- `kontra.profile(df)` - Profile data structure and statistics
"""

PROBE_DOCS_FULL = """## Kontra Functions
- `kontra.validate(df, contract_path)` - Validate data against a contract
- `kontra.profile(df)` - Profile data structure and statistics

### Transformation Probes (CRITICAL FOR MULTI-STEP PIPELINES!)

**BEFORE any JOIN**, use `profile_relationship()`:
```python
profile = kontra.profile_relationship(left_df, right_df, on="join_key")
print(profile.to_llm())
```
This reveals orphan keys and duplicate keys that affect joins.

**AFTER EACH PIPELINE STEP**, use `compare()` to verify the transformation:
```python
cmp = kontra.compare(before_df, after_df, key="id_column")
print(cmp.to_llm())
```

This is CRITICAL because:
- `row_stats.before` vs `row_stats.after` - Did row count change as expected?
- `key_stats.dropped` - Did you accidentally lose records?
- `key_stats.duplicated_after` - Did you accidentally create duplicates?

**REQUIRED WORKFLOW FOR THIS PIPELINE:**

```python
# Step 1: Deduplicate
step1 = deduplicate(events)
cmp1 = kontra.compare(events, step1, key="event_id")
print("After dedup:", cmp1.to_llm())
# VERIFY: row count decreased, no duplicates in step1

# Step 2: Enrich with users
profile = kontra.profile_relationship(step1, users, on="user_id")
print("Join profile:", profile.to_llm())
step2 = step1.join(users, on="user_id", how="inner")
cmp2 = kontra.compare(step1, step2, key="event_id")
print("After enrich:", cmp2.to_llm())
# VERIFY: understand how many records lost to orphan user_ids

# Step 3: Filter completed
step3 = step2.filter(...)  # Handle status casing!
cmp3 = kontra.compare(step2, step3, key="event_id")
print("After filter:", cmp3.to_llm())
# VERIFY: row count decreased appropriately

# Step 4: Aggregate
step4 = step3.group_by(...).agg(...)
# Can't compare directly (different key), but verify row count is reasonable
```

If ANY step shows unexpected results, FIX IT before proceeding!
Validating only at the end makes debugging MUCH harder.
"""

# =============================================================================
# Execution environment
# =============================================================================

def create_execution_env():
    """Create a fresh execution environment for the agent."""
    import polars as pl
    import kontra

    env = {
        "pl": pl,
        "kontra": kontra,
        "__builtins__": __builtins__,
    }
    return env


def execute_code(code: str, env: dict) -> str:
    """Execute Python code and return output."""
    import io
    import contextlib

    stdout = io.StringIO()

    try:
        os.chdir(EXPERIMENT_DIR)

        with contextlib.redirect_stdout(stdout):
            exec(code, env)

        output = stdout.getvalue()
        if not output:
            output = "(Code executed successfully, no output)"
        return output

    except Exception as e:
        error_output = stdout.getvalue()
        return f"{error_output}\nError: {type(e).__name__}: {e}"


# =============================================================================
# Agent loop
# =============================================================================

@dataclass
class ExperimentResult:
    """Results from a single experiment run."""
    condition: str
    success: bool
    iterations: int
    total_code_executions: int
    validation_attempts: int
    used_compare: bool = False
    used_profile_relationship: bool = False
    compare_calls: int = 0
    final_validation_passed: bool = False
    messages: list = field(default_factory=list)
    code_blocks: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def to_dict(self) -> dict:
        return {
            "condition": self.condition,
            "success": self.success,
            "iterations": self.iterations,
            "total_code_executions": self.total_code_executions,
            "validation_attempts": self.validation_attempts,
            "used_compare": self.used_compare,
            "used_profile_relationship": self.used_profile_relationship,
            "compare_calls": self.compare_calls,
            "final_validation_passed": self.final_validation_passed,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


def run_agent(condition: str, verbose: bool = True) -> ExperimentResult:
    """Run the agent experiment with native tool calling."""
    client = OpenAI()

    # Build system prompt
    if condition == "control":
        probe_docs = PROBE_DOCS_NONE
    else:
        probe_docs = PROBE_DOCS_FULL

    system_prompt = SYSTEM_PROMPT_BASE.format(probe_docs=probe_docs)

    result = ExperimentResult(
        condition=condition,
        success=False,
        iterations=0,
        total_code_executions=0,
        validation_attempts=0,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Please solve the task. This is a multi-step pipeline - work through each step carefully."},
    ]

    tools = [TOOL_EXECUTE_PYTHON, TOOL_DECLARE_SUCCESS]
    env = create_execution_env()

    if verbose:
        print(f"\n{'='*60}")
        print(f"Running experiment: {condition.upper()}")
        print(f"{'='*60}\n")

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1

        if verbose:
            print(f"\n--- Iteration {iteration + 1} ---")

        # Get agent response (with retry for rate limits)
        import time
        for attempt in range(3):
            try:
                response = client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    temperature=0.1,
                    max_tokens=4000,
                )
                break
            except Exception as e:
                if "rate_limit" in str(e).lower() and attempt < 2:
                    if verbose:
                        print(f"Rate limited, waiting 10s...")
                    time.sleep(10)
                    continue
                result.errors.append(f"API error: {e}")
                if verbose:
                    print(f"API Error: {e}")
                break
        else:
            break

        # Track tokens
        if response.usage:
            result.prompt_tokens += response.usage.prompt_tokens
            result.completion_tokens += response.usage.completion_tokens
            result.total_tokens += response.usage.total_tokens

        assistant_message = response.choices[0].message
        messages.append(assistant_message)

        # Print assistant's reasoning (if any)
        if assistant_message.content and verbose:
            print(f"Agent thinking: {assistant_message.content[:300]}...")

        # Check for tool calls
        if not assistant_message.tool_calls:
            if verbose:
                print("No tool calls, prompting to continue...")
            messages.append({
                "role": "user",
                "content": "Please use the execute_python tool to continue working on the task."
            })
            continue

        # Process each tool call
        for tool_call in assistant_message.tool_calls:
            function_name = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)

            if verbose:
                print(f"Tool: {function_name}")

            if function_name == "execute_python":
                code = function_args["code"]
                result.code_blocks.append(code)
                result.total_code_executions += 1

                # Track probe usage
                if "kontra.compare(" in code:
                    result.used_compare = True
                    result.compare_calls += code.count("kontra.compare(")
                if "kontra.profile_relationship(" in code:
                    result.used_profile_relationship = True
                if "kontra.validate(" in code:
                    result.validation_attempts += 1

                if verbose:
                    print(f"Code:\n{code[:300]}...")

                output = execute_code(code, env)

                if verbose:
                    print(f"Output:\n{output[:500]}...")

                # Check for validation pass (various output formats)
                if ("passed=True" in output or
                    "passed: True" in output or
                    "Passed: True" in output or
                    "Passed:True" in output or
                    output.strip().startswith("True")):
                    result.final_validation_passed = True

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": output
                })

            elif function_name == "declare_success":
                explanation = function_args.get("explanation", "")
                if verbose:
                    print(f"SUCCESS declared: {explanation}")

                if result.final_validation_passed:
                    result.success = True
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": "Success confirmed. Experiment complete."
                    })
                else:
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": "Cannot declare success - validation has not passed yet. Please run kontra.validate() and ensure result.passed == True."
                    })

        if result.success:
            break

    if verbose:
        print(f"\n{'='*60}")
        print(f"Experiment complete: {condition}")
        print(f"Success: {result.success}")
        print(f"Iterations: {result.iterations}")
        print(f"Code executions: {result.total_code_executions}")
        print(f"Validation attempts: {result.validation_attempts}")
        print(f"Total tokens: {result.total_tokens:,} (prompt: {result.prompt_tokens:,}, completion: {result.completion_tokens:,})")
        if condition == "treatment":
            print(f"Used compare(): {result.used_compare} ({result.compare_calls} calls)")
            print(f"Used profile_relationship(): {result.used_profile_relationship}")
        print(f"{'='*60}\n")

    return result


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Run agent transformation experiment")
    parser.add_argument("--condition", choices=["control", "treatment"],
                       help="Which condition to run")
    parser.add_argument("--both", action="store_true",
                       help="Run both conditions and compare")
    parser.add_argument("--quiet", action="store_true",
                       help="Suppress verbose output")

    args = parser.parse_args()

    if not args.condition and not args.both:
        parser.print_help()
        sys.exit(1)

    verbose = not args.quiet

    if args.both:
        print("\n" + "="*70)
        print("RUNNING A/B EXPERIMENT: EVENT PIPELINE (MULTI-STEP)")
        print("="*70)

        control = run_agent("control", verbose=verbose)
        treatment = run_agent("treatment", verbose=verbose)

        print("\n" + "="*70)
        print("RESULTS COMPARISON")
        print("="*70)
        print(f"\n{'Metric':<30} {'Control':<15} {'Treatment':<15}")
        print("-"*60)
        print(f"{'Success':<30} {str(control.success):<15} {str(treatment.success):<15}")
        print(f"{'Iterations':<30} {control.iterations:<15} {treatment.iterations:<15}")
        print(f"{'Code executions':<30} {control.total_code_executions:<15} {treatment.total_code_executions:<15}")
        print(f"{'Validation attempts':<30} {control.validation_attempts:<15} {treatment.validation_attempts:<15}")
        print(f"{'Total tokens':<30} {control.total_tokens:<15,} {treatment.total_tokens:<15,}")
        print(f"{'Prompt tokens':<30} {control.prompt_tokens:<15,} {treatment.prompt_tokens:<15,}")
        print(f"{'Completion tokens':<30} {control.completion_tokens:<15,} {treatment.completion_tokens:<15,}")
        print(f"{'Used compare()':<30} {'-':<15} {str(treatment.used_compare):<15}")
        print(f"{'compare() calls':<30} {'-':<15} {treatment.compare_calls:<15}")
        print(f"{'Used profile_relationship()':<30} {'-':<15} {str(treatment.used_profile_relationship):<15}")
        print("-"*60)

        # Save results
        results = {
            "timestamp": datetime.now().isoformat(),
            "experiment": "event-pipeline",
            "control": control.to_dict(),
            "treatment": treatment.to_dict(),
        }

        results_file = EXPERIMENT_DIR / "results.json"
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to {results_file}")

    else:
        result = run_agent(args.condition, verbose=verbose)

        results_file = EXPERIMENT_DIR / f"result_{args.condition}.json"
        with open(results_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"\nResults saved to {results_file}")


if __name__ == "__main__":
    main()
