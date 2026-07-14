#!/usr/bin/env python
"""AI-slop checker for the Kontra codebase.

Scans Python sources for patterns that indicate generated-code slop:

  Correctness / debt (CLAUDE.md invariants):
    bare-except          bare `except:`
    broad-except         `except Exception:` without logging or re-raise
    locals-check         `if x in locals()` anti-pattern
    heavy-import         top-level import of polars/duckdb/psycopg/pymssql

  Noise:
    narrating-comment    comment that restates the following line
    section-comment      banner comments like `# ----- Helpers -----` overuse
    obvious-docstring    docstring that merely restates the function name
    redundant-else       `else` directly after a branch that returns/raises
    trivial-fstring      f-string with no placeholders
    bool-compare         `== True`, `== False`, `!= None`, `== None`
    print-call           print() in library code (src/ only)
    todo                 TODO/FIXME/XXX markers

  Structure:
    unused-import        import never referenced in the module
    dup-function         near-identical function bodies within a module
    long-function        function > 80 statements
    deep-nesting         nesting depth > 5

Usage:
    python scripts/slop_check.py [paths...] [--json] [--only CAT[,CAT]] [--summary]
Exit code 0 always (it's a report, not a gate).
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
import tokenize
from collections import Counter
from dataclasses import dataclass, asdict
from pathlib import Path

HEAVY_MODULES = {"polars", "duckdb", "psycopg", "pymssql", "pandas", "pyarrow"}

# Verbs that open a comment which usually just narrates the next line.
NARRATE_VERBS = (
    "import", "initialize", "init", "create", "return", "set", "get",
    "call", "check", "loop", "iterate", "define", "declare", "increment",
    "append", "add", "update", "convert", "parse", "build", "make",
    "instantiate", "assign", "extract", "compute", "calculate", "now",
    "first", "then", "finally", "try", "use", "we", "this",
)

WORD_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


@dataclass
class Finding:
    path: str
    line: int
    category: str
    detail: str

    def render(self) -> str:
        return f"{self.path}:{self.line}: [{self.category}] {self.detail}"


def _words(text: str) -> set[str]:
    out = set()
    for w in WORD_RE.findall(text.lower()):
        out.add(w)
        # split snake_case / camelCase-ish tokens
        out.update(p for p in w.split("_") if len(p) > 2)
    return out


def check_comments(path: Path, source: str, findings: list[Finding]) -> None:
    lines = source.splitlines()
    try:
        tokens = list(tokenize.generate_tokens(iter(lines_iter(lines)).__next__))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return
    banner_count = 0
    for tok in tokens:
        if tok.type != tokenize.COMMENT:
            continue
        text = tok.string.lstrip("#").strip()
        lineno = tok.start[0]
        if not text:
            continue
        low = text.lower()
        if re.fullmatch(r"[-=~_* ]{8,}|[-=~_* ]*[a-z0-9 /&_.]+[-=~_* ]{4,}", low) and len(low) > 12:
            banner_count += 1
            continue
        if re.search(r"\b(todo|fixme|xxx|hack)\b", low):
            findings.append(Finding(str(path), lineno, "todo", text[:90]))
            continue
        # narrating comment: starts with a narration verb AND shares most of
        # its identifiers with the next non-blank code line
        first = low.split(" ", 1)[0].rstrip(":,")
        if first in NARRATE_VERBS:
            code_after = tok.line.split("#")[0].strip()
            target = code_after
            if not target:  # standalone comment: look at next code line
                for nxt in lines[lineno : lineno + 3]:
                    s = nxt.strip()
                    if s and not s.startswith("#"):
                        target = s
                        break
            cwords = _words(text) - {"the", "a", "an", "to", "of", "for", "and", "if", "is"}
            twords = _words(target)
            if cwords and len(cwords & twords) / len(cwords) >= 0.6:
                findings.append(Finding(str(path), lineno, "narrating-comment", text[:90]))


def lines_iter(lines):
    for ln in lines:
        yield ln + "\n"


class SlopVisitor(ast.NodeVisitor):
    def __init__(self, path: Path, source: str, findings: list[Finding], is_src: bool):
        self.path = str(path)
        self.source = source
        self.findings = findings
        self.is_src = is_src
        self.imported: dict[str, int] = {}       # name -> lineno
        self.used_names: set[str] = set()
        self.func_bodies: dict[str, list[tuple[str, int]]] = {}
        self.depth = 0
        self.in_type_checking = False
        self.has_all = "__all__" in source

    def add(self, node: ast.AST, cat: str, detail: str) -> None:
        self.findings.append(Finding(self.path, getattr(node, "lineno", 0), cat, detail))

    # --- imports ---------------------------------------------------------
    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            root = alias.name.split(".")[0]
            bound = alias.asname or alias.name.split(".")[0]
            self.imported[bound] = node.lineno
            if self.depth == 0 and not self.in_type_checking and root in HEAVY_MODULES and self.is_src:
                self.add(node, "heavy-import", f"top-level import of {root}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "__future__":
            return
        root = (node.module or "").split(".")[0]
        if self.depth == 0 and not self.in_type_checking and root in HEAVY_MODULES and self.is_src:
            self.add(node, "heavy-import", f"top-level from-import of {root}")
        for alias in node.names:
            if alias.name == "*":
                continue
            self.imported[alias.asname or alias.name] = node.lineno
        self.generic_visit(node)

    def visit_If(self, node: ast.If) -> None:
        guard = "TYPE_CHECKING" in ast.unparse(node.test)
        if guard:
            self.in_type_checking = True
        self.generic_visit(node)
        if guard:
            self.in_type_checking = False

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.used_names.add(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self.generic_visit(node)

    # --- exceptions ------------------------------------------------------
    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.type is None:
            self.add(node, "bare-except", "bare `except:`")
        elif isinstance(node.type, ast.Name) and node.type.id in ("Exception", "BaseException"):
            body_src = ast.unparse(ast.Module(body=node.body, type_ignores=[]))
            if not re.search(r"\b(log|logger|logging|warn|raise|_logger)\b", body_src):
                self.add(node, "broad-except",
                         f"`except {node.type.id}` swallowed without log/raise")
        self.generic_visit(node)

    # --- misc expressions --------------------------------------------------
    def visit_Compare(self, node: ast.Compare) -> None:
        for op, comp in zip(node.ops, node.comparators):
            if isinstance(comp, ast.Constant):
                if comp.value is True or comp.value is False:
                    if isinstance(op, (ast.Eq, ast.NotEq)):
                        self.add(node, "bool-compare", f"comparison with {comp.value}")
                elif comp.value is None and isinstance(op, (ast.Eq, ast.NotEq)):
                    self.add(node, "bool-compare", "use `is None` / `is not None`")
            if (isinstance(comp, ast.Call) and isinstance(comp.func, ast.Name)
                    and comp.func.id == "locals"):
                self.add(node, "locals-check", "`in locals()` anti-pattern")
        self.generic_visit(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        if not any(isinstance(v, ast.FormattedValue) for v in node.values):
            self.add(node, "trivial-fstring", "f-string with no placeholders")
        self.generic_visit(node)

    def visit_FormattedValue(self, node: ast.FormattedValue) -> None:
        # visit only the expression; format_spec is itself a JoinedStr and
        # would be falsely reported as a placeholder-less f-string
        self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name) and node.func.id == "print" and self.is_src:
            self.add(node, "print-call", "print() in library code")
        self.generic_visit(node)

    # --- functions ---------------------------------------------------------
    def _visit_func(self, node) -> None:
        self.depth += 1
        doc = ast.get_docstring(node)
        if doc:
            first = doc.strip().splitlines()[0].rstrip(".").lower()
            name_words = {w for w in node.name.lower().split("_") if w}
            if name_words and _words(first) and name_words == (_words(first) & name_words) \
                    and len(_words(first)) <= len(name_words) + 2:
                self.add(node, "obvious-docstring",
                         f"docstring of {node.name}() restates its name: {doc.splitlines()[0][:60]!r}")
        n_stmts = sum(1 for _ in ast.walk(node) if isinstance(_, ast.stmt))
        if n_stmts > 80:
            self.add(node, "long-function", f"{node.name}() has {n_stmts} statements")
        # duplicate detection: hash of normalized body
        try:
            body_src = ast.unparse(ast.Module(body=node.body, type_ignores=[]))
        except Exception:  # unparse edge cases  # noqa: BLE001
            body_src = ""
        if len(body_src) > 120:
            h = hashlib.md5(re.sub(r"\s+", " ", body_src).encode()).hexdigest()
            self.func_bodies.setdefault(h, []).append((node.name, node.lineno))
        self._check_nesting(node)
        self._check_redundant_else(node)
        self.generic_visit(node)
        self.depth -= 1

    visit_FunctionDef = _visit_func
    visit_AsyncFunctionDef = _visit_func

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.depth += 1
        self.generic_visit(node)
        self.depth -= 1

    def _check_nesting(self, func) -> None:
        def depth_of(node, d=0):
            best = d
            for child in ast.iter_child_nodes(node):
                nd = d + 1 if isinstance(child, (ast.If, ast.For, ast.While, ast.With, ast.Try)) else d
                best = max(best, depth_of(child, nd))
            return best
        d = depth_of(func)
        if d > 5:
            self.add(func, "deep-nesting", f"{func.name}() nests {d} levels deep")

    def _check_redundant_else(self, func) -> None:
        for node in ast.walk(func):
            if isinstance(node, ast.If) and node.orelse and node.body:
                last = node.body[-1]
                # only flag plain else (not elif) to keep signal high
                is_elif = len(node.orelse) == 1 and isinstance(node.orelse[0], ast.If)
                if isinstance(last, (ast.Return, ast.Raise, ast.Continue, ast.Break)) and not is_elif:
                    self.findings.append(Finding(
                        self.path, node.orelse[0].lineno, "redundant-else",
                        "else after return/raise/continue/break"))

    def finish(self) -> None:
        # A name is "used" if it appears anywhere in the source other than its
        # own import line — this keeps string annotations ("pl.DataFrame"),
        # docstring references, and __all__ entries from being false positives.
        word_lines: dict[str, set[int]] = {}
        for i, line in enumerate(self.source.splitlines(), 1):
            for w in WORD_RE.findall(line):
                word_lines.setdefault(w, set()).add(i)
        src_lines = self.source.splitlines()
        for name, lineno in self.imported.items():
            base = name.split(".")[0]
            if self.path.endswith("__init__.py") or self.has_all:
                continue
            if word_lines.get(base, set()) - {lineno}:
                continue
            if lineno <= len(src_lines) and "noqa" in src_lines[lineno - 1]:
                continue  # explicitly marked (e.g. typing re-exports)
            self.findings.append(Finding(self.path, lineno, "unused-import",
                                         f"`{name}` imported but unused"))
        for h, funcs in self.func_bodies.items():
            if len(funcs) > 1:
                names = ", ".join(f"{n}:{ln}" for n, ln in funcs)
                self.findings.append(Finding(self.path, funcs[0][1], "dup-function",
                                             f"identical bodies: {names}"))

    def source_dunder_all(self) -> set[str]:
        m = re.search(r"__all__\s*=\s*\[([^\]]*)\]", self.source, re.S)
        if not m:
            return set()
        return set(re.findall(r"['\"]([^'\"]+)['\"]", m.group(1)))


def scan_file(path: Path) -> list[Finding]:
    findings: list[Finding] = []
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        return [Finding(str(path), 0, "read-error", str(e))]
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return [Finding(str(path), e.lineno or 0, "syntax-error", str(e))]
    is_src = "/src/" in str(path) or str(path).startswith("src/")
    visitor = SlopVisitor(path, source, findings, is_src)
    visitor.visit(tree)
    visitor.finish()
    check_comments(path, source, findings)
    return findings


def import_time_modules() -> set[str] | None:
    """Modules loaded by a bare `import kontra` (None if kontra not importable)."""
    import subprocess
    code = ("import json, sys; import kontra; "
            "print(json.dumps([m for m in sys.modules if m.startswith('kontra')]))")
    try:
        out = subprocess.run([sys.executable, "-c", code], capture_output=True,
                             text=True, timeout=30, cwd="src")
        if out.returncode != 0:
            return None
        return set(json.loads(out.stdout.strip().splitlines()[-1]))
    except (subprocess.SubprocessError, ValueError, IndexError):
        return None


def main(argv: list[str]) -> int:
    args = [a for a in argv if not a.startswith("--")]
    as_json = "--json" in argv
    summary_only = "--summary" in argv
    only = None
    for a in argv:
        if a.startswith("--only"):
            only = set(a.split("=", 1)[1].split(",")) if "=" in a else None
    roots = [Path(a) for a in args] or [Path("src")]
    files: list[Path] = []
    for r in roots:
        files.extend(sorted(r.rglob("*.py")) if r.is_dir() else [r])

    all_findings: list[Finding] = []
    for f in files:
        if "__pycache__" in str(f):
            continue
        all_findings.extend(scan_file(f))

    # heavy-import only matters on the `import kontra` chain: executors,
    # materializers, backends etc. are themselves lazily loaded, so their
    # top-level heavy imports are by design. Filter to import-time modules.
    eager = import_time_modules()
    if eager is not None:
        def is_eager(fi: Finding) -> bool:
            mod = str(Path(fi.path)).replace("src/", "").replace("/", ".").removesuffix(".py")
            mod = mod.removesuffix(".__init__")
            return mod in eager
        all_findings = [f for f in all_findings
                        if f.category != "heavy-import" or is_eager(f)]
    if only:
        all_findings = [f for f in all_findings if f.category in only]

    counts = Counter(f.category for f in all_findings)
    per_file = Counter(f.path for f in all_findings)

    if as_json:
        print(json.dumps([asdict(f) for f in all_findings], indent=1))
        return 0
    if not summary_only:
        for f in all_findings:
            print(f.render())
        print()
    print(f"== {len(all_findings)} findings in {len(per_file)} files ==")
    for cat, n in counts.most_common():
        print(f"  {cat:20s} {n}")
    print("-- worst files --")
    for p, n in per_file.most_common(15):
        print(f"  {n:4d}  {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
