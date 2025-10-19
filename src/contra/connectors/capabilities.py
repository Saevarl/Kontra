# src/contra/connectors/capabilities.py
from __future__ import annotations
from enum import IntFlag

class ConnectorCapabilities(IntFlag):
    """
    Bitmask describing what a connector/source can do.

    Keep this *very* small and stable. The engine uses it only as a hint to
    pick executors/materializers; behavior must remain correct even if caps=NONE.

    Bits:
      - NONE            : no special capabilities
      - LOCAL           : local filesystem access (fast, cheap seeks)
      - PUSHDOWN        : can push predicates / projections to the source
      - REMOTE_PARTIAL  : remote store that benefits from partial reads (S3, ADLS)
      - SQL_ENDPOINT    : source is a SQL engine we can push queries to (Postgres, Snowflake)
    """
    NONE           = 0
    LOCAL          = 1 << 0
    PUSHDOWN       = 1 << 1
    REMOTE_PARTIAL = 1 << 2
    SQL_ENDPOINT   = 1 << 3

# Backwards-compat aliases (keep existing imports happy)
CC = ConnectorCapabilities
