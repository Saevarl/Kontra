"""
Agent Transformation Experiment: Session Revenue (5-Step Pipeline)

Fair A/B test of Kontra's transformation probes (compare, profile_relationship).

Design:
- Both agents get: neutral memory system, context summarization
- Only treatment gets: compare(), profile_relationship()
- This isolates the variable to just probes

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

from dotenv import load_dotenv
from openai import OpenAI

# Load .env file from project root
load_dotenv(Path(__file__).parent.parent.parent / ".env")

# =============================================================================
# Configuration
# =============================================================================

MAX_ITERATIONS = 20
MODEL = "gpt-5-mini"
SUMMARIZER_MODEL = "gpt-5-nano"  # Cheap/fast for summarization
MAX_CONTEXT_TOKENS = 32000  # Summarize when exceeded
TOKEN_ESTIMATE_CHARS = 4  # ~4 chars per token

EXPERIMENT_DIR = Path(__file__).parent
DATA_DIR = EXPERIMENT_DIR / "data"

# =============================================================================
# Neutral Memory System (same for both agents)
# =============================================================================

class AgentMemory:
    """Simple key-value memory that both agents can use equally."""

    def __init__(self):
        self.notes: dict[str, str] = {}

    def write(self, key: str, value: str) -> str:
        self.notes[key] = value
        return f"Saved note '{key}'"

    def read_all(self) -> str:
        if not self.notes:
            return "No notes saved yet."
        return "\n".join(f"- {k}: {v}" for k, v in self.notes.items())

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
        "description": "Call this IMMEDIATELY when validation passes (result.passed == True).",
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

TOOL_MEMORY_WRITE = {
    "type": "function",
    "function": {
        "name": "memory_write",
        "description": "Save a note to memory. Use this to record observations, plans, or findings that you want to remember.",
        "parameters": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "A short label for this note (e.g., 'step1_result', 'trap_found')"
                },
                "value": {
                    "type": "string",
                    "description": "The content to remember"
                }
            },
            "required": ["key", "value"]
        }
    }
}

TOOL_MEMORY_READ = {
    "type": "function",
    "function": {
        "name": "memory_read_all",
        "description": "Read all saved notes from memory.",
        "parameters": {
            "type": "object",
            "properties": {}
        }
    }
}

# =============================================================================
# System prompts
# =============================================================================

SYSTEM_PROMPT_BASE = """You are a data engineer solving a complex multi-step pipeline.

## Task
Create a pipeline that produces `session_summary` from e-commerce clickstream data:

**Step 1: Clean bot traffic** - Filter using is_bot flag (but CHECK the data first!)
**Step 2: Deduplicate clicks** - Remove duplicate clicks
**Step 3: Enrich with products** - Join to get prices
**Step 4: Calculate session metrics** - Aggregate by session_id
**Step 5: Enrich with users** - Join to get user attributes

## Target Schema
- `session_id`: Unique session identifier
- `user_id`: User identifier
- `user_name`: From users table
- `user_segment`: From users table
- `session_duration_seconds`: Duration in seconds (must be >= 0!)
- `click_count`: Number of clicks (>= 1)
- `purchase_count`: Number of purchases (>= 0)
- `total_revenue`: Sum of product prices for purchases (>= 0)

## Data
- `data/clickstream.parquet` - Click events (click_id, session_id, user_id, product_id, event_type, timestamp_seconds, is_bot)
  - NOTE: timestamp_seconds is epoch seconds (integer) - easy math: max - min = duration
- `data/products.parquet` - Product catalog (product_id, product_name, price, category)
- `data/users.parquet` - User attributes (user_id, user_name, signup_date, user_segment)

## Success Criteria
```python
result = kontra.validate(output_df, "target_contract.yml")
# result.passed must be True
```

## Available Libraries
- polars (imported as pl)
  - NOTE: Use `df.group_by()` not `df.groupby()`
  - NOTE: For conditional aggregation use `pl.when().then().otherwise()`:
    ```python
    .agg([
        pl.col('user_id').first().alias('user_id'),
        (pl.col('timestamp_seconds').max() - pl.col('timestamp_seconds').min()).alias('duration'),
        pl.len().alias('click_count'),
        pl.when(pl.col('event_type') == 'purchase').then(1).otherwise(0).sum().alias('purchase_count'),
        pl.when(pl.col('event_type') == 'purchase').then(pl.col('price')).otherwise(0).sum().alias('revenue'),
    ])
    ```
- kontra

## Memory System
You have a memory system to record observations:
- `memory_write(key, value)` - Save a note
- `memory_read_all()` - Read all notes

Use this to track what you've tried and learned!

{probe_docs}

## CRITICAL WARNINGS
- This pipeline has MULTIPLE TRAPS that compound
- A problem in step 1 affects ALL downstream steps
- THINK CAREFULLY about data quality at each step
- Check data distributions before filtering!

## When to Stop
When validation passes (result.passed == True): call `declare_success()` IMMEDIATELY.
"""

PROBE_DOCS_NONE = """## Kontra Functions
- `kontra.validate(df, contract_path)` - Validate data against a contract
- `kontra.profile(df)` - Profile data structure and statistics
"""

PROBE_DOCS_FULL = """## Kontra Functions
- `kontra.validate(df, contract_path)` - Validate data against a contract
- `kontra.profile(df)` - Profile data structure and statistics

### Transformation Probes (USE THESE!)

**BEFORE any JOIN**, use `profile_relationship()`:
```python
profile = kontra.profile_relationship(left_df, right_df, on="join_key")
print(profile.to_llm())
```
- Check `coverage.left_keys_without_match` - orphan keys that will become NULL
- Check `key_stats.right.duplicate_keys` - will cause row explosion!

**AFTER EACH STEP**, use `compare()`:
```python
cmp = kontra.compare(before_df, after_df, key="key_column")
print(cmp.to_llm())
```
- Check `row_stats.delta` - Did row count change as expected?
- Check `row_stats.ratio` - What fraction of data survived?

**KEY INSIGHT:** In a multi-step pipeline, a problem in step 1 corrupts ALL downstream steps.
Using compare() after EACH step lets you catch problems immediately.
"""

# =============================================================================
# Context Summarization
# =============================================================================

def estimate_tokens(messages: list) -> int:
    """Rough token estimate: ~4 chars per token."""
    total_chars = sum(len(json.dumps(m)) for m in messages)
    return total_chars // TOKEN_ESTIMATE_CHARS


def summarize_context(client: OpenAI, messages: list, memory: AgentMemory) -> list:
    """Summarize conversation if it exceeds token limit."""
    current_tokens = estimate_tokens(messages)

    if current_tokens <= MAX_CONTEXT_TOKENS:
        return messages  # No summarization needed

    # Keep system prompt and last 3 messages
    system_msg = messages[0]
    recent_msgs = messages[-3:] if len(messages) > 3 else messages[1:]
    middle_msgs = messages[1:-3] if len(messages) > 4 else []

    if not middle_msgs:
        return messages  # Nothing to summarize

    # Summarize middle messages
    middle_content = "\n\n".join(
        f"[{m.get('role', 'unknown')}]: {str(m.get('content', ''))[:500]}"
        for m in middle_msgs
    )

    try:
        summary_response = client.chat.completions.create(
            model=SUMMARIZER_MODEL,
            messages=[{
                "role": "user",
                "content": f"""Summarize this conversation history concisely (max 300 words).
Focus on: what was attempted, what worked, what failed, current state.

{middle_content[:20000]}"""
            }],
            max_completion_tokens=400,
        )
        summary = summary_response.choices[0].message.content
    except Exception as e:
        summary = f"(Summarization failed: {e})"

    # Build summarized context
    memory_state = memory.read_all()

    return [
        system_msg,
        {
            "role": "user",
            "content": f"""[CONTEXT SUMMARY]
{summary}

[YOUR MEMORY NOTES]
{memory_state}

Continue working on the task. Remember: call declare_success() when validation passes."""
        },
        *recent_msgs
    ]

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

        # Truncate very long outputs
        if len(output) > 3000:
            output = output[:2500] + f"\n... (truncated {len(output) - 2500} chars)"

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
    memory_writes: int = 0
    used_compare: bool = False
    used_profile_relationship: bool = False
    compare_calls: int = 0
    profile_relationship_calls: int = 0
    summarizations: int = 0
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
            "memory_writes": self.memory_writes,
            "used_compare": self.used_compare,
            "used_profile_relationship": self.used_profile_relationship,
            "compare_calls": self.compare_calls,
            "profile_relationship_calls": self.profile_relationship_calls,
            "summarizations": self.summarizations,
            "final_validation_passed": self.final_validation_passed,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


def run_agent(condition: str, verbose: bool = True) -> ExperimentResult:
    """Run the agent experiment with fair design."""
    client = OpenAI()
    memory = AgentMemory()  # Same for both conditions

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
        {"role": "user", "content": "Please solve the task. Work through each step carefully."},
    ]

    # Both agents get memory tools
    tools = [TOOL_EXECUTE_PYTHON, TOOL_DECLARE_SUCCESS, TOOL_MEMORY_WRITE, TOOL_MEMORY_READ]
    env = create_execution_env()

    if verbose:
        print(f"\n{'='*60}")
        print(f"Running experiment: {condition.upper()}")
        print(f"{'='*60}\n")

    for iteration in range(MAX_ITERATIONS):
        result.iterations = iteration + 1

        if verbose:
            print(f"\n--- Iteration {iteration + 1} ---")

        # Summarize context if needed
        old_len = len(messages)
        messages = summarize_context(client, messages, memory)
        if len(messages) < old_len:
            result.summarizations += 1
            if verbose:
                print(f"(Context summarized: {old_len} -> {len(messages)} messages)")

        # Get agent response
        import time
        response = None
        for attempt in range(3):
            try:
                response = client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",
                    max_completion_tokens=4000,
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

        if response is None:
            break

        # Track tokens
        if response.usage:
            result.prompt_tokens += response.usage.prompt_tokens
            result.completion_tokens += response.usage.completion_tokens
            result.total_tokens += response.usage.total_tokens

        assistant_message = response.choices[0].message

        # Convert to dict for storage (avoid serialization issues)
        msg_dict = {"role": "assistant", "content": assistant_message.content}
        if assistant_message.tool_calls:
            msg_dict["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments}
                }
                for tc in assistant_message.tool_calls
            ]
        messages.append(msg_dict)

        # Print assistant's reasoning
        if assistant_message.content and verbose:
            print(f"Agent: {assistant_message.content[:200]}...")

        # Check for tool calls
        if not assistant_message.tool_calls:
            if verbose:
                print("No tool calls, prompting...")
            messages.append({
                "role": "user",
                "content": "Use execute_python to continue, or declare_success if validation passed."
            })
            continue

        # Process tool calls
        should_exit = False
        for tool_call in assistant_message.tool_calls:
            function_name = tool_call.function.name
            function_args = json.loads(tool_call.function.arguments)

            if verbose:
                print(f"Tool: {function_name}")

            if function_name == "execute_python":
                code = function_args["code"]
                result.code_blocks.append(code)
                result.total_code_executions += 1

                # Track probe usage (treatment only)
                if "kontra.compare(" in code:
                    result.used_compare = True
                    result.compare_calls += code.count("kontra.compare(")
                if "kontra.profile_relationship(" in code:
                    result.used_profile_relationship = True
                    result.profile_relationship_calls += code.count("kontra.profile_relationship(")
                if "kontra.validate(" in code:
                    result.validation_attempts += 1

                if verbose:
                    print(f"Code:\n{code[:200]}...")

                output = execute_code(code, env)

                if verbose:
                    print(f"Output:\n{output[:300]}...")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": output
                })

            elif function_name == "declare_success":
                explanation = function_args.get("explanation", "")
                if verbose:
                    print(f"SUCCESS: {explanation}")

                result.success = True
                result.final_validation_passed = True
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": "Success confirmed."
                })
                should_exit = True
                break

            elif function_name == "memory_write":
                key = function_args.get("key", "note")
                value = function_args.get("value", "")
                output = memory.write(key, value)
                result.memory_writes += 1

                if verbose:
                    print(f"Memory: {key} = {value[:50]}...")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": output
                })

            elif function_name == "memory_read_all":
                output = memory.read_all()

                if verbose:
                    print(f"Memory read: {output[:100]}...")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": output
                })

        if should_exit:
            break

    if verbose:
        print(f"\n{'='*60}")
        print(f"Experiment complete: {condition}")
        print(f"Success: {result.success}")
        print(f"Iterations: {result.iterations}")
        print(f"Code executions: {result.total_code_executions}")
        print(f"Memory writes: {result.memory_writes}")
        print(f"Summarizations: {result.summarizations}")
        print(f"Total tokens: {result.total_tokens:,}")
        if condition == "treatment":
            print(f"compare() calls: {result.compare_calls}")
            print(f"profile_relationship() calls: {result.profile_relationship_calls}")
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
        print("FAIR A/B EXPERIMENT: SESSION REVENUE PIPELINE")
        print("Both agents get: memory system, context summarization")
        print("Only treatment gets: compare(), profile_relationship()")
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
        print(f"{'Memory writes':<30} {control.memory_writes:<15} {treatment.memory_writes:<15}")
        print(f"{'Summarizations':<30} {control.summarizations:<15} {treatment.summarizations:<15}")
        print(f"{'Total tokens':<30} {control.total_tokens:<15,} {treatment.total_tokens:<15,}")
        print(f"{'compare() calls':<30} {'-':<15} {treatment.compare_calls:<15}")
        print(f"{'profile_relationship() calls':<30} {'-':<15} {treatment.profile_relationship_calls:<15}")
        print("-"*60)

        # Save results
        results = {
            "timestamp": datetime.now().isoformat(),
            "experiment": "session-revenue-fair",
            "design": {
                "both_have": ["memory_system", "context_summarization"],
                "treatment_only": ["compare()", "profile_relationship()"],
            },
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
