"""DuckDB views over the normalized Parquet tree.

Every table of :data:`tickvault.models.TABLES` gets a view backed by
``read_parquet('<root>/norm/<table>/**/*.parquet', hive_partitioning = true)``
with the hive partition types pinned: ``date`` is ``DATE`` (DuckDB's own
inference for ``date=YYYY-MM-DD`` directories, and what the QA queries filter
on), ``hour`` is ``INTEGER`` (so ``hour=05`` never turns into the string
``'05'``) and ``venue`` is ``VARCHAR``. The ``venue`` column that every table
already stores comes from the file; the hive key is pinned for completeness.

DuckDB refuses to bind ``read_parquet`` when the glob matches no files, so a
table without any file gets a typed zero-row view built from the Arrow schema
in :mod:`tickvault.models` (table columns in schema order, then ``date`` and
``hour``, matching the column order of the populated views). ``SELECT ... FROM
<table>`` therefore never errors on a fresh data directory.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from tickvault.models import TABLES

_HIVE_TYPES = "{'venue': 'VARCHAR', 'date': 'DATE', 'hour': 'INTEGER'}"


def view_ddl(root: str | Path) -> str:
    """Return the ``CREATE VIEW`` statements for ``root``, one per table.

    The text is a pure function of the data tree: a table with files gets the
    ``read_parquet`` statement, a table without files the typed empty-view
    statement.
    """
    return "\n".join(_view_statements(root)) + "\n"


def connect(root: str | Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the four views created."""
    connection = duckdb.connect()
    for statement in _view_statements(root):
        connection.execute(statement)
    return connection


def run_query(root: str | Path, sql: str) -> tuple[list[str], list[tuple[object, ...]]]:
    """Create the views on a fresh connection, run ``sql`` and return its rows."""
    connection = duckdb.connect()
    try:
        for statement in _view_statements(root):
            connection.execute(statement)
        result = connection.execute(sql)
        columns = [description[0] for description in result.description or []]
        rows = [tuple(row) for row in result.fetchall()]
    finally:
        connection.close()
    return columns, rows


def _view_statements(root: str | Path) -> list[str]:
    root_path = Path(root)
    return [_create_view(root_path, table) for table in TABLES]


def _create_view(root: Path, table: str) -> str:
    directory = root / "norm" / table
    if any(directory.rglob("*.parquet")):
        pattern = (directory / "**" / "*.parquet").as_posix()
        return (
            f"CREATE VIEW {table} AS SELECT * FROM "
            f"read_parquet({_sql_quote(pattern)}, hive_partitioning = true, "
            f"hive_types = {_HIVE_TYPES});"
        )
    columns = [_null_column(field) for field in TABLES[table]]
    columns.append("CAST(NULL AS DATE) AS date")
    columns.append("CAST(NULL AS INTEGER) AS hour")
    return f"CREATE VIEW {table} AS SELECT {', '.join(columns)} WHERE FALSE;"


def _null_column(field: pa.Field[pa.DataType]) -> str:
    return f"CAST(NULL AS {_duckdb_type(field.type)}) AS {field.name}"


def _duckdb_type(data_type: pa.DataType) -> str:
    if pa.types.is_string(data_type):
        return "VARCHAR"
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_int16(data_type):
        return "SMALLINT"
    if isinstance(data_type, pa.Decimal128Type):
        return f"DECIMAL({data_type.precision}, {data_type.scale})"
    raise TypeError(f"no DuckDB type mapping for Arrow type {data_type}")


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
