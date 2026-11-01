"""The route fuzzer needs hypothesis (a dev dependency); skip the folder without it."""

import importlib.util

if importlib.util.find_spec("hypothesis") is None:
    collect_ignore_glob = ["test_*.py"]
