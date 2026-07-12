import json
import os
import subprocess
import sys


def test_import_kontra_does_not_import_mcp_or_psycopg():
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    code = """
import json, sys
import kontra
print(json.dumps({
    'mcp': any(n == 'mcp' or n.startswith('mcp.') for n in sys.modules),
    'psycopg': any(n == 'psycopg' or n.startswith('psycopg.') for n in sys.modules),
    'kontra_mcp': any(n.startswith('kontra.mcp') for n in sys.modules),
}))
"""
    proc = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    loaded = json.loads(proc.stdout)
    assert loaded == {"mcp": False, "psycopg": False, "kontra_mcp": False}
