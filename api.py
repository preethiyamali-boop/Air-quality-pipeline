from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query


PROJECT_ROOT = Path(__file__).resolve().parent
MAX_LIMIT = 1_000


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _find_data_source() -> tuple[str, str]:
    configured = os.getenv("AIR_QUALITY_DATA")
    candidates = []
    if configured:
        candidates.append(Path(configured))

    candidates.extend(
        [
            PROJECT_ROOT / "partitioned_clean_data",
            PROJECT_ROOT / "team_4_clean_strict.parquet",
            PROJECT_ROOT / "team_4_clean.parquet",
            PROJECT_ROOT / "team_4.parquet",
        ]
    )

    for candidate in candidates:
        if candidate.is_dir():
            pattern = (candidate / "**" / "*.parquet").as_posix()
            return (
                str(candidate),
                f"read_parquet({_sql_string(pattern)}, hive_partitioning=true)",
            )
        if candidate.is_file():
            return str(candidate), f"read_parquet({_sql_string(candidate.as_posix())})"

    raise FileNotFoundError(
        "No parquet data source found. Create partitioned_clean_data/ or place "
        "team_4_clean_strict.parquet, team_4_clean.parquet, or team_4.parquet in the project root."
    )


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    clean = df.astype(object).where(pd.notna(df), None)
    return clean.to_dict(orient="records")


def _where_clause(
    station_id: str | None = None,
    pollutant: str | None = None,
    year: int | None = None,
    month: int | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if station_id:
        clauses.append("station_id = ?")
        params.append(station_id)
    if pollutant:
        clauses.append("pollutant = ?")
        params.append(pollutant)
    if year:
        clauses.append("year = ?")
        params.append(year)
    if month:
        clauses.append("month = ?")
        params.append(month)

    return ("WHERE " + " AND ".join(clauses), params) if clauses else ("", params)


app = FastAPI(
    title="Air Quality Data Serving API",
    description="Serves cleaned Delhi air-quality data through DuckDB-backed analytical endpoints.",
    version="1.0.0",
)


@app.on_event("startup")
def startup() -> None:
    try:
        data_path, table_expr = _find_data_source()
    except FileNotFoundError as exc:
        app.state.data_error = str(exc)
        app.state.data_path = None
        app.state.table_expr = None
        app.state.con = None
        return

    app.state.data_error = None
    app.state.data_path = data_path
    app.state.table_expr = table_expr
    app.state.con = duckdb.connect(database=":memory:", read_only=False)


def _connection() -> duckdb.DuckDBPyConnection:
    if getattr(app.state, "data_error", None):
        raise HTTPException(status_code=503, detail=app.state.data_error)
    return app.state.con


@app.get("/")
def health() -> dict[str, Any]:
    return {
        "status": "ok" if not getattr(app.state, "data_error", None) else "missing_data",
        "data_source": getattr(app.state, "data_path", None),
        "docs": "/docs",
    }


@app.get("/metadata")
def metadata() -> dict[str, Any]:
    con = _connection()
    table_expr = app.state.table_expr
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MIN(datetime) AS start_time,
            MAX(datetime) AS end_time,
            COUNT(DISTINCT station_id) AS station_count,
            COUNT(DISTINCT pollutant) AS pollutant_count
        FROM {table_expr}
        """
    ).fetchone()

    return {
        "row_count": row[0],
        "start_time": row[1],
        "end_time": row[2],
        "station_count": row[3],
        "pollutant_count": row[4],
        "data_source": app.state.data_path,
    }


@app.get("/stations")
def stations() -> list[dict[str, Any]]:
    con = _connection()
    df = con.execute(
        f"""
        SELECT DISTINCT station_id, city, state
        FROM {app.state.table_expr}
        ORDER BY station_id
        """
    ).fetchdf()
    return _records(df)


@app.get("/pollutants")
def pollutants() -> list[dict[str, Any]]:
    con = _connection()
    df = con.execute(
        f"""
        SELECT pollutant, COUNT(*) AS measurement_count
        FROM {app.state.table_expr}
        GROUP BY pollutant
        ORDER BY measurement_count DESC
        """
    ).fetchdf()
    return _records(df)


@app.get("/measurements")
def measurements(
    station_id: str | None = None,
    pollutant: str | None = None,
    year: int | None = Query(default=None, ge=2024, le=2025),
    month: int | None = Query(default=None, ge=1, le=12),
    limit: int = Query(default=100, ge=1, le=MAX_LIMIT),
) -> list[dict[str, Any]]:
    con = _connection()
    where_sql, params = _where_clause(station_id, pollutant, year, month)
    params.append(limit)
    df = con.execute(
        f"""
        SELECT *
        FROM {app.state.table_expr}
        {where_sql}
        ORDER BY datetime
        LIMIT ?
        """,
        params,
    ).fetchdf()
    return _records(df)


@app.get("/summary/monthly")
def monthly_summary(
    pollutant: str = Query(default="pm25"),
    station_id: str | None = None,
) -> list[dict[str, Any]]:
    con = _connection()
    where_sql, params = _where_clause(station_id=station_id, pollutant=pollutant)
    df = con.execute(
        f"""
        SELECT
            year,
            month,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            COUNT(*) AS measurement_count
        FROM {app.state.table_expr}
        {where_sql}
        GROUP BY year, month
        ORDER BY year, month
        """,
        params,
    ).fetchdf()
    return _records(df)


@app.get("/summary/stations")
def station_summary(
    pollutant: str = Query(default="pm10"),
    year: int | None = Query(default=None, ge=2024, le=2025),
    month: int | None = Query(default=None, ge=1, le=12),
    limit: int = Query(default=10, ge=1, le=MAX_LIMIT),
) -> list[dict[str, Any]]:
    con = _connection()
    where_sql, params = _where_clause(pollutant=pollutant, year=year, month=month)
    params.append(limit)
    df = con.execute(
        f"""
        SELECT
            station_id,
            AVG(value) AS avg_value,
            MIN(value) AS min_value,
            MAX(value) AS max_value,
            COUNT(*) AS measurement_count
        FROM {app.state.table_expr}
        {where_sql}
        GROUP BY station_id
        ORDER BY avg_value DESC
        LIMIT ?
        """,
        params,
    ).fetchdf()
    return _records(df)
