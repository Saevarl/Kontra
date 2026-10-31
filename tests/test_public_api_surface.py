"""
Guard the public ``kontra`` namespace.

The snapshot in ``public_api_surface.json`` records every public name on the
package right after ``import kontra``, plus the signature, defining module
and docstring hash of each public callable, and ``__all__``. Moving code
between modules must not change any of it.

Regenerate (only for an intentional API change):

    python tests/test_public_api_surface.py --update
"""

import json
import subprocess
import sys
from pathlib import Path

SNAPSHOT = Path(__file__).with_name("public_api_surface.json")

_COLLECT = r"""
import hashlib, inspect, json
import kontra

def describe(obj):
    entry = {"type": type(obj).__name__}
    if inspect.ismodule(obj):
        entry["module"] = obj.__name__
        return entry
    entry["module"] = getattr(obj, "__module__", None)
    if callable(obj) and entry["module"] and entry["module"].startswith("kontra"):
        try:
            entry["signature"] = str(inspect.signature(obj))
        except (TypeError, ValueError):
            entry["signature"] = None
        doc = inspect.getdoc(obj) or ""
        entry["doc_sha256"] = hashlib.sha256(doc.encode()).hexdigest()
    return entry

names = sorted(n for n in dir(kontra) if not n.startswith("_"))
surface = {n: describe(getattr(kontra, n)) for n in names}
exported = {n: describe(getattr(kontra, n)) for n in kontra.__all__}
print(json.dumps({
    "names": names,
    "surface": surface,
    "all": list(kontra.__all__),
    "all_resolved": exported,
}, indent=2, sort_keys=True))
"""


def collect_surface() -> dict:
    """Collect the surface in a fresh interpreter so no prior import leaks in."""
    proc = subprocess.run(
        [sys.executable, "-c", _COLLECT],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def test_public_names_unchanged():
    expected = json.loads(SNAPSHOT.read_text())
    actual = collect_surface()
    assert actual["names"] == expected["names"]
    assert actual["all"] == expected["all"]


def test_public_callables_unchanged():
    expected = json.loads(SNAPSHOT.read_text())
    actual = collect_surface()
    for key in ("surface", "all_resolved"):
        for name, entry in expected[key].items():
            assert actual[key].get(name) == entry, f"kontra.{name} changed"


def test_help_reports_kontra_module():
    """help(kontra.validate) etc. keep saying 'in module kontra'."""
    import pydoc

    import kontra

    for name in ("validate", "profile", "diff", "annotate", "health"):
        text = pydoc.render_doc(getattr(kontra, name), renderer=pydoc.plaintext)
        assert f"function {name} in module kontra\n" in text


def test_private_names_used_through_package():
    """Internal callers reach these through ``kontra``; keep them there."""
    import kontra

    assert callable(kontra._normalize_to_dataframe)  # kontra.probes.utils
    original = kontra.get_config_path()
    try:
        kontra.set_config(__file__)
        assert kontra._config_path_override == __file__
        kontra.set_config(None)
        assert kontra._config_path_override is None
    finally:
        kontra.set_config(original)


if __name__ == "__main__":
    if "--update" in sys.argv:
        SNAPSHOT.write_text(json.dumps(collect_surface(), indent=2, sort_keys=True) + "\n")
        print(f"wrote {SNAPSHOT}")
