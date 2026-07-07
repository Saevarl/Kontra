# src/kontra/preplan/parquet_meta.py
"""
Pure-Python Parquet footer reader for the metadata preplan.

Parses the thrift-compact-encoded FileMetaData at the end of a Parquet file
and extracts exactly what the preplan needs: schema names/types, row counts,
and per-row-group min/max/null_count statistics.

Why not pyarrow: importing pyarrow.parquet costs ~175ms, which dominates
cold-start for local-file validations. This module reads a footer in ~1ms
with stdlib only. It is deliberately conservative: any value it cannot
decode with certainty comes back as None, which the planner treats as
"no stats" and defers to the SQL/Polars tiers. The planner falls back to
pyarrow entirely if parsing raises ParquetMetaError.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional

# Parquet physical types (format spec: Type)
_BOOLEAN, _INT32, _INT64, _INT96, _FLOAT, _DOUBLE, _BYTE_ARRAY, _FLBA = range(8)

# Parquet ConvertedType values used here
_CT_UTF8 = 0
_CT_DECIMAL = 5
_CT_DATE = 6
_CT_TIME_MILLIS = 7
_CT_TIME_MICROS = 8
_CT_TIMESTAMP_MILLIS = 9
_CT_TIMESTAMP_MICROS = 10
_CT_UINT_8, _CT_UINT_16, _CT_UINT_32, _CT_UINT_64 = 11, 12, 13, 14
_CT_INT_8, _CT_INT_16, _CT_INT_32, _CT_INT_64 = 15, 16, 17, 18

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


class ParquetMetaError(Exception):
    """Footer could not be parsed; caller should fall back to pyarrow."""


@dataclass
class _Logical:
    """Distilled logical/converted type info for one schema leaf."""

    kind: Optional[str] = None  # "string" | "date" | "timestamp" | "time" | "int" | "decimal" | ...
    unit: Optional[str] = None  # millis | micros | nanos (time/timestamp)
    utc: bool = False  # isAdjustedToUTC (timestamp)
    bit_width: int = 0  # integer
    signed: bool = True  # integer
    precision: int = 0  # decimal
    scale: int = 0  # decimal


@dataclass
class _SchemaLeaf:
    name: str  # dotted path for nested columns
    physical: int = -1
    logical: _Logical = field(default_factory=_Logical)


@dataclass
class ParquetMeta:
    """The subset of Parquet FileMetaData the preplan consumes."""

    num_rows: int
    schema_names: List[str]  # leaf column names (dotted paths)
    schema_types: Dict[str, str]  # top-level name -> normalized dtype string
    # Per row group: column name -> {"min": ..., "max": ..., "null_count": int?}
    # min/max are typed Python values (int/float/str/bool/date/datetime/time).
    row_groups: List[Dict[str, Dict[str, Any]]]

    @property
    def num_row_groups(self) -> int:
        return len(self.row_groups)


# ---------------------------------------------------------------------------
# Thrift compact protocol
# ---------------------------------------------------------------------------

_T_STOP = 0
_T_TRUE = 1
_T_FALSE = 2
_T_BYTE = 3
_T_I16 = 4
_T_I32 = 5
_T_I64 = 6
_T_DOUBLE = 7
_T_BINARY = 8
_T_LIST = 9
_T_SET = 10
_T_MAP = 11
_T_STRUCT = 12


class _Reader:
    """Minimal thrift compact protocol reader over a bytes buffer."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def u8(self) -> int:
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def varint(self) -> int:
        result = 0
        shift = 0
        while True:
            b = self.u8()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7
            if shift > 70:
                raise ParquetMetaError("varint too long")

    def zigzag(self) -> int:
        v = self.varint()
        return (v >> 1) ^ -(v & 1)

    def read_bytes(self) -> bytes:
        n = self.varint()
        end = self.pos + n
        if end > len(self.buf):
            raise ParquetMetaError("binary length past end of footer")
        out = self.buf[self.pos : end]
        self.pos = end
        return out

    def field_header(self, last_fid: int) -> tuple:
        """Return (ftype, fid); ftype == _T_STOP terminates the struct."""
        byte = self.u8()
        if byte == 0:
            return (_T_STOP, 0)
        delta = (byte >> 4) & 0x0F
        ftype = byte & 0x0F
        fid = last_fid + delta if delta else self.zigzag()
        return (ftype, fid)

    def list_header(self) -> tuple:
        byte = self.u8()
        size = (byte >> 4) & 0x0F
        etype = byte & 0x0F
        if size == 15:
            size = self.varint()
        # Every compact-protocol list element occupies >= 1 byte, so a size
        # beyond the remaining buffer is a lie (hostile or corrupt footer).
        if size > len(self.buf) - self.pos:
            raise ParquetMetaError("list size exceeds footer bounds")
        return (size, etype)

    def skip(self, ftype: int) -> None:
        if ftype in (_T_TRUE, _T_FALSE):
            return  # value lives in the field header
        if ftype == _T_BYTE:
            self.pos += 1
        elif ftype in (_T_I16, _T_I32, _T_I64):
            self.varint()
        elif ftype == _T_DOUBLE:
            self.pos += 8
        elif ftype == _T_BINARY:
            n = self.varint()  # read length first: varint() itself moves pos
            self.pos += n
        elif ftype in (_T_LIST, _T_SET):
            size, etype = self.list_header()
            # Fixed-width elements skip arithmetically: a hostile size varint
            # must not buy a CPU-burn loop from a tiny footer.
            if etype in (_T_TRUE, _T_FALSE, _T_BYTE):
                self.pos += size
            elif etype == _T_DOUBLE:
                self.pos += 8 * size
            else:
                for _ in range(size):
                    self.skip_element(etype)
        elif ftype == _T_MAP:
            size = self.varint()
            if size > len(self.buf) - self.pos:
                raise ParquetMetaError("map size exceeds footer bounds")
            if size:
                kv = self.u8()
                ktype, vtype = (kv >> 4) & 0x0F, kv & 0x0F
                for _ in range(size):
                    self.skip_element(ktype)
                    self.skip_element(vtype)
        elif ftype == _T_STRUCT:
            self.skip_struct()
        else:
            raise ParquetMetaError(f"cannot skip thrift type {ftype}")

    def skip_element(self, etype: int) -> None:
        # In lists, booleans occupy one byte (unlike struct fields).
        if etype in (_T_TRUE, _T_FALSE):
            self.pos += 1
        else:
            self.skip(etype)

    def skip_struct(self) -> None:
        last_fid = 0
        while True:
            ftype, fid = self.field_header(last_fid)
            if ftype == _T_STOP:
                return
            self.skip(ftype)
            last_fid = fid


# ---------------------------------------------------------------------------
# FileMetaData parsing
# ---------------------------------------------------------------------------


def _parse_logical_type(r: _Reader) -> _Logical:
    """Parse the LogicalType union struct."""
    lg = _Logical()
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            return lg
        if fid == 1:
            lg.kind = "string"
            r.skip(ftype)
        elif fid == 5:  # DECIMAL
            lg.kind = "decimal"
            sub = 0
            while True:
                st, sf = r.field_header(sub)
                if st == _T_STOP:
                    break
                if sf == 1:
                    lg.scale = r.zigzag()
                elif sf == 2:
                    lg.precision = r.zigzag()
                else:
                    r.skip(st)
                sub = sf
        elif fid == 6:
            lg.kind = "date"
            r.skip(ftype)
        elif fid in (7, 8):  # TIME / TIMESTAMP
            lg.kind = "time" if fid == 7 else "timestamp"
            sub = 0
            while True:
                st, sf = r.field_header(sub)
                if st == _T_STOP:
                    break
                if sf == 1:  # isAdjustedToUTC
                    lg.utc = st == _T_TRUE
                elif sf == 2:  # unit: union of empty structs keyed by field id
                    unit_fid = 0
                    while True:
                        ut, uf = r.field_header(unit_fid)
                        if ut == _T_STOP:
                            break
                        lg.unit = {1: "millis", 2: "micros", 3: "nanos"}.get(uf)
                        r.skip(ut)
                        unit_fid = uf
                else:
                    r.skip(st)
                sub = sf
        elif fid == 10:  # INTEGER
            lg.kind = "int"
            sub = 0
            while True:
                st, sf = r.field_header(sub)
                if st == _T_STOP:
                    break
                if sf == 1:
                    lg.bit_width = r.u8()
                elif sf == 2:
                    lg.signed = st == _T_TRUE
                else:
                    r.skip(st)
                sub = sf
        elif fid == 15:
            lg.kind = "float16"
            r.skip(ftype)
        else:  # MAP/LIST/ENUM/UNKNOWN/JSON/BSON/UUID — not stats-relevant
            lg.kind = lg.kind or "other"
            r.skip(ftype)
        last_fid = fid


def _parse_schema_element(r: _Reader) -> tuple:
    """Return (name, physical, num_children, logical)."""
    name = ""
    physical = -1
    num_children = 0
    converted = -1
    logical: Optional[_Logical] = None
    scale = precision = 0
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            break
        if fid == 1:
            physical = r.zigzag()
        elif fid == 4:
            name = r.read_bytes().decode("utf-8", errors="replace")
        elif fid == 5:
            num_children = r.zigzag()
        elif fid == 6:
            converted = r.zigzag()
        elif fid == 7:
            scale = r.zigzag()
        elif fid == 8:
            precision = r.zigzag()
        elif fid == 10:
            logical = _parse_logical_type(r)
        else:
            r.skip(ftype)
        last_fid = fid

    # LogicalType (new) wins; otherwise distill ConvertedType (legacy).
    if logical is None:
        logical = _Logical()
        if converted == _CT_UTF8:
            logical.kind = "string"
        elif converted == _CT_DATE:
            logical.kind = "date"
        elif converted in (_CT_TIME_MILLIS, _CT_TIME_MICROS):
            logical.kind = "time"
            logical.unit = "millis" if converted == _CT_TIME_MILLIS else "micros"
        elif converted in (_CT_TIMESTAMP_MILLIS, _CT_TIMESTAMP_MICROS):
            logical.kind = "timestamp"
            logical.unit = "millis" if converted == _CT_TIMESTAMP_MILLIS else "micros"
            logical.utc = True  # converted TIMESTAMP_* is UTC-normalized per spec
        elif _CT_UINT_8 <= converted <= _CT_UINT_64:
            logical.kind = "int"
            logical.signed = False
            logical.bit_width = 8 << (converted - _CT_UINT_8)
        elif _CT_INT_8 <= converted <= _CT_INT_64:
            logical.kind = "int"
            logical.bit_width = 8 << (converted - _CT_INT_8)
        elif converted == _CT_DECIMAL:
            logical.kind = "decimal"
            logical.scale = scale
            logical.precision = precision
    elif logical.kind == "decimal" and not logical.precision:
        logical.precision, logical.scale = precision, scale
    return (name, physical, num_children, logical)


def _parse_statistics(r: _Reader) -> Dict[str, Any]:
    """Return raw Statistics fields: min/max (new), min_dep/max_dep, null_count."""
    out: Dict[str, Any] = {}
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            return out
        if fid == 1:
            out["max_dep"] = r.read_bytes()
        elif fid == 2:
            out["min_dep"] = r.read_bytes()
        elif fid == 3:
            out["null_count"] = r.zigzag()
        elif fid == 5:
            out["max"] = r.read_bytes()
        elif fid == 6:
            out["min"] = r.read_bytes()
        else:
            r.skip(ftype)
        last_fid = fid


def _parse_column_meta(r: _Reader) -> tuple:
    """Return (path_in_schema, physical, stats_dict_or_None)."""
    path: List[str] = []
    physical = -1
    stats: Optional[Dict[str, Any]] = None
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            return (path, physical, stats)
        if fid == 1:
            physical = r.zigzag()
        elif fid == 3:
            size, _etype = r.list_header()
            path = [r.read_bytes().decode("utf-8", errors="replace") for _ in range(size)]
        elif fid == 12:
            stats = _parse_statistics(r)
        else:
            r.skip(ftype)
        last_fid = fid


def _parse_column_chunk(r: _Reader) -> Optional[tuple]:
    """Return (path, physical, stats) from the chunk's ColumnMetaData, if present."""
    meta = None
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            return meta
        if fid == 3:
            meta = _parse_column_meta(r)
        else:
            r.skip(ftype)
        last_fid = fid


def _parse_row_group(r: _Reader) -> List[Optional[tuple]]:
    """Return list of per-column (path, physical, stats) tuples."""
    columns: List[Optional[tuple]] = []
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            return columns
        if fid == 1:
            size, _etype = r.list_header()
            columns = [_parse_column_chunk(r) for _ in range(size)]
        else:
            r.skip(ftype)
        last_fid = fid


# ---------------------------------------------------------------------------
# Value decoding
# ---------------------------------------------------------------------------


def _decode_stat(raw: bytes, physical: int, lg: _Logical, *, ceil: bool = False) -> Any:
    """
    Decode one PLAIN-encoded statistics value. Returns None for anything we
    cannot decode with certainty (decimals, INT96, raw binary, float16).

    ceil: nanosecond values don't fit datetime's microsecond precision, so we
    round toward a WIDER [min, max] interval — floor for min, ceil for max.
    A widened bound can only turn a provable decision into "unknown" (safe);
    plain truncation could move max below the true max and prove a false pass.
    """
    try:
        if physical == _BOOLEAN:
            return raw[0] != 0
        if physical == _INT32:
            if lg.kind == "decimal":
                return None
            if lg.kind == "int" and not lg.signed:
                return struct.unpack("<I", raw)[0]
            v = struct.unpack("<i", raw)[0]
            if lg.kind == "date":
                return _EPOCH_DATE + timedelta(days=v)
            if lg.kind == "time":  # TIME_MILLIS
                return (_EPOCH_NAIVE + timedelta(milliseconds=v)).time()
            return v
        if physical == _INT64:
            if lg.kind == "decimal":
                return None
            if lg.kind == "int" and not lg.signed:
                return struct.unpack("<Q", raw)[0]
            v = struct.unpack("<q", raw)[0]
            if lg.kind == "timestamp":
                return _int64_to_datetime(v, lg, ceil=ceil)
            if lg.kind == "time":
                if lg.unit == "nanos":
                    v = _ns_to_us(v, ceil=ceil)
                return (_EPOCH_NAIVE + timedelta(microseconds=v)).time()
            return v
        if physical == _FLOAT:
            return struct.unpack("<f", raw)[0]
        if physical == _DOUBLE:
            return struct.unpack("<d", raw)[0]
        if physical == _BYTE_ARRAY:
            if lg.kind == "string":
                return raw.decode("utf-8")
            return None  # raw binary: not comparable to rule values
        # INT96 timestamps and FLBA (decimal/uuid/float16): decline
        return None
    except (struct.error, IndexError, UnicodeDecodeError, OverflowError):
        return None


def _ns_to_us(v: int, *, ceil: bool) -> int:
    q, r = divmod(v, 1000)  # floor semantics hold for negatives too
    if ceil and r:
        q += 1
    return q


def _int64_to_datetime(v: int, lg: _Logical, *, ceil: bool = False) -> Optional[datetime]:
    epoch = _EPOCH_UTC if lg.utc else _EPOCH_NAIVE
    if lg.unit == "millis":
        return epoch + timedelta(milliseconds=v)
    if lg.unit == "micros":
        return epoch + timedelta(microseconds=v)
    if lg.unit == "nanos":
        return epoch + timedelta(microseconds=_ns_to_us(v, ceil=ceil))
    return None


# Deprecated (pre-2.6) min/max are only trustworthy where signed byte-wise
# comparison matches the type's sort order — signed numerics and booleans.
# Strings/decimals/unsigned from old writers had broken orderings; decline.
def _deprecated_ok(physical: int, lg: _Logical) -> bool:
    if lg.kind in ("decimal", "float16"):
        return False
    if lg.kind == "int" and not lg.signed:
        return False
    return physical in (_BOOLEAN, _INT32, _INT64, _FLOAT, _DOUBLE)


def _normalized_type(physical: int, lg: _Logical) -> str:
    """Normalized dtype string matching planner's pyarrow-based names."""
    if lg.kind == "string":
        return "string"
    if lg.kind == "date":
        return "date"
    if lg.kind == "timestamp":
        return "datetime"
    if lg.kind == "time":
        return "time"
    if lg.kind == "decimal":
        return f"decimal128({lg.precision}, {lg.scale})"
    if lg.kind == "float16":
        return "float32"  # arrow "halffloat" normalizes to float32
    if lg.kind == "int" and lg.bit_width:
        return f"{'int' if lg.signed else 'uint'}{lg.bit_width}"
    return {
        _BOOLEAN: "boolean",
        _INT32: "int32",
        _INT64: "int64",
        _INT96: "datetime",
        _FLOAT: "float32",
        _DOUBLE: "float64",
        _BYTE_ARRAY: "binary",
    }.get(physical, "unknown")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def read_parquet_meta(path: str) -> ParquetMeta:
    """
    Read Parquet footer metadata from a local file using stdlib only.

    Raises ParquetMetaError (or OSError from I/O) if the file is not a
    readable, unencrypted Parquet file; callers fall back to pyarrow.
    """
    with open(path, "rb") as f:
        f.seek(0, 2)
        file_len = f.tell()
        if file_len < 12:
            raise ParquetMetaError("file too small to be Parquet")
        f.seek(file_len - 8)
        tail = f.read(8)
        if tail[4:] != b"PAR1":
            raise ParquetMetaError(f"bad footer magic {tail[4:]!r} (encrypted or not Parquet)")
        footer_len = struct.unpack("<I", tail[:4])[0]
        if footer_len > file_len - 8:
            raise ParquetMetaError("footer length exceeds file size")
        f.seek(file_len - 8 - footer_len)
        footer = f.read(footer_len)

    try:
        return _parse_file_meta(footer)
    except (IndexError, struct.error) as e:
        raise ParquetMetaError(f"corrupt or truncated footer: {e}") from e


def _parse_file_meta(footer: bytes) -> ParquetMeta:
    r = _Reader(footer)
    num_rows = 0
    schema_elems: List[tuple] = []
    raw_row_groups: List[List[Optional[tuple]]] = []
    last_fid = 0
    while True:
        ftype, fid = r.field_header(last_fid)
        if ftype == _T_STOP:
            break
        if fid == 2:  # schema: flattened tree, depth-first
            size, _etype = r.list_header()
            schema_elems = [_parse_schema_element(r) for _ in range(size)]
        elif fid == 3:
            num_rows = r.zigzag()
        elif fid == 4:  # row_groups
            size, _etype = r.list_header()
            raw_row_groups = [_parse_row_group(r) for _ in range(size)]
        else:
            r.skip(ftype)
        last_fid = fid

    if not schema_elems:
        raise ParquetMetaError("no schema in footer")

    # Walk the flattened schema tree: root's num_children top-level fields,
    # each subtree contributing leaves (dotted paths).
    leaves: Dict[str, _SchemaLeaf] = {}
    schema_names: List[str] = []
    schema_types: Dict[str, str] = {}

    idx = 1  # skip root

    def walk(prefix: str, top_level: bool) -> None:
        nonlocal idx
        name, physical, num_children, lg = schema_elems[idx]
        idx += 1
        full = f"{prefix}.{name}" if prefix else name
        if num_children:
            # Nested (struct/list/map) top-level fields get no schema_types
            # entry: dtype rules on them defer to exact tiers ("unknown").
            for _ in range(num_children):
                walk(full, False)
        else:
            leaf = _SchemaLeaf(name=full, physical=physical, logical=lg)
            leaves[full] = leaf
            # Bare leaf name, matching pyarrow's ParquetSchema.names: nested
            # leaves must NOT satisfy the planner's dotted-column existence
            # check, or the pure and pyarrow paths make different decisions.
            schema_names.append(name)
            if top_level:
                schema_types[name] = _normalized_type(physical, lg)

    _root_name, _root_phys, root_children, _root_lg = schema_elems[0]
    for _ in range(root_children):
        if idx >= len(schema_elems):
            raise ParquetMetaError("schema tree shorter than declared")
        walk("", True)

    row_groups: List[Dict[str, Dict[str, Any]]] = []
    for raw_rg in raw_row_groups:
        per_col: Dict[str, Dict[str, Any]] = {}
        for chunk in raw_rg:
            if chunk is None:
                continue
            path, physical, stats = chunk
            if stats is None:
                continue
            name = ".".join(path)
            leaf = leaves.get(name)
            lg = leaf.logical if leaf else _Logical()
            if physical < 0 and leaf:
                physical = leaf.physical

            min_raw, max_raw = stats.get("min"), stats.get("max")
            if min_raw is None and max_raw is None and _deprecated_ok(physical, lg):
                min_raw, max_raw = stats.get("min_dep"), stats.get("max_dep")

            entry: Dict[str, Any] = {
                "min": _decode_stat(min_raw, physical, lg) if min_raw is not None else None,
                "max": _decode_stat(max_raw, physical, lg, ceil=True) if max_raw is not None else None,
            }
            if "null_count" in stats:
                entry["null_count"] = stats["null_count"]
            per_col[name] = entry
        row_groups.append(per_col)

    return ParquetMeta(
        num_rows=num_rows,
        schema_names=schema_names,
        schema_types=schema_types,
        row_groups=row_groups,
    )
