# Air Quality Data Pipeline — Team 4

Delhi air-quality sensor data (2024–2025). 5.8M rows across 10 monitoring stations, 13 pollutants. Built as part of a multi-week data engineering course at IITM Zanzibar.

The instructor ingested raw CSVs from 8 sources, joined them into one master dataset, and split across 8 teams. This repo is Team 4's work from that point forward.

---

## Dataset at a glance

| Property | Value |
|---|---|
| Source | CPCB air-quality monitoring stations |
| Location | Delhi, India |
| Time range | January 2024 – December 2025 |
| Size | 5,831,431 rows × 22 columns |
| Format received | Parquet |
| Stations | 10 |
| Pollutants | 13 (8 CPCB standard + 5 VOCs) |

**Pollutants:** PM2.5, PM10, NO2, SO2, CO, O3, NH3, benzene, toluene, xylene, mp-xylene, ethylbenzene, NO

**Weather columns also present:** temperature, humidity, wind speed/direction, rainfall, solar radiation, barometric pressure

---

## Week 1 — Ingestion & Partitioning

**Goal:** Load the parquet, produce an ingestion summary, partition the data by year and month.

### What we did

1. Loaded `team_4.parquet` into a pandas DataFrame using pyarrow
2. Generated an ingestion summary — row count, date range, stations, pollutants, missing-value audit
3. Partitioned into 24 files using Hive-style folder layout
4. Verified partitioning is lossless (all 5,831,431 rows recoverable)

### Partition structure

```
partitioned_data/
  year=2024/
    month=01/data.parquet   ← ~243K rows
    month=02/data.parquet
    ...
  year=2025/
    month=12/data.parquet
```

Folder names encode the partition key — pandas, Spark, DuckDB, and Hive all read this layout natively. Loading just one month means touching one small file instead of 5.8M rows.

### Data quality flags

| Column | Issue |
|---|---|
| `vws_m_s` | 100% null — drop entirely |
| `ws_m_s`, `sr_w_mt2`, `at_c`, `bp_mmhg`, `rf_mm` | 30–45% null — needs imputation |
| Pollutant count | 13 in file vs "8 main species" mentioned by instructor — VOCs need filtering decision |

### Gotcha we hit

First version of the partition kept `year` and `month` columns inside each file AND encoded them in folder names. Reading the full dataset back threw a type-mismatch error (int32 vs dictionary type). Fix: drop those columns before saving — the folder names already carry that information.

---

## Week 2 — Data Model & Database

**Goal:** Design a schema, load into a database, and prove the choice was right with actual benchmark numbers.

### Why DuckDB

Three options were evaluated:

| Option | Fit | Reason |
|---|---|---|
| DBMS (PostgreSQL, SQLite) | Partial | Row-based storage — slower for analytical queries on large datasets |
| NoSQL (MongoDB) | Poor | Built for flexible/unstructured data; ours is strictly structured |
| Columnar (DuckDB, ClickHouse) | ✅ Best | Column-by-column storage matches our parquet format and analytical workload |

DuckDB specifically because: zero server setup, runs inside Python, reads parquet natively, standard SQL, right-sized for 5.8M rows.

### Empirical proof — DuckDB vs SQLite benchmark

Same dataset, same queries, two databases:

| Metric | SQLite (row-based) | DuckDB (columnar) | DuckDB (parquet direct) |
|---|---|---|---|
| Load time | 71.5 seconds | 19.1 seconds | n/a |
| File size | 1078.3 MB | 80.5 MB | 51.8 MB |
| Q1: Avg PM2.5/month | 1852.7 ms | 86.8 ms | 117.5 ms |
| Q2: Count/pollutant | 7051.7 ms | 164.4 ms | 51.9 ms |
| **Speed vs SQLite** | baseline | **21–43× faster** | **16–136× faster** |
| **Size vs SQLite** | baseline | **13.4× smaller** | **20.8× smaller** |

DuckDB reads only the columns a query needs and skips the rest. SQLite reads every row whole regardless. On Q2 (count per pollutant), only one column is needed out of 22 — DuckDB skips 95% of the data. That gap shows in the numbers.

### Schema — star schema

```
     ┌──────────────┐              ┌───────────────┐
     │   stations   │              │  pollutants   │
     │  (10 rows)   │              │  (13 rows)    │
     ├──────────────┤              ├───────────────┤
     │ 🔑 station_id│              │ 🔑 pollutant_id│
     │ city         │              │ pollutant_name│
     │ state        │              └──────┬────────┘
     └──────┬───────┘                     │
            │ 1                         1 │
            │ ∞                         ∞ │
            ▼                             ▼
     ┌────────────────────────────────────────────┐
     │              measurements                  │
     │              (5,831,431 rows)              │
     ├────────────────────────────────────────────┤
     │ 🔑 measurement_id   BIGINT                 │
     │ 🔗 station_id       → stations             │
     │ 🔗 pollutant_id     → pollutants           │
     │    datetime         TIMESTAMP              │
     │    value            DOUBLE                 │
     │    year, month, day, hour  INTEGER         │
     │    at_c, rh_percent, bp_mmhg  DOUBLE       │
     │    ws_m_s, wd_deg, rf_mm, sr_w_mt2  DOUBLE │
     └────────────────────────────────────────────┘
```

Normalizing into three tables means "Delhi" is stored 10 times (once per station) instead of 5.8 million times. Pollutant names are stored 13 times instead of ~460,000 times each.

### Query benchmarks

All run on Google Colab free tier, May 2026.

| Query | Time | Rows returned |
|---|---|---|
| Avg PM2.5 per month | 86.8 ms | 24 |
| Top 3 stations by avg PM10 | 82.5 ms | 3 |
| Measurement count per pollutant | 164.4 ms | 13 |
| Daily peak NO2 for one station | 56.7 ms | 716 |

All queries under 165ms on 5.8M rows.

---

## Week 3 — Cleaning, Auditing, Partitioning & Database Comparison

**Goal:** Clean the dataset, show empty/duplicate rows, partition the cleaned data, and compare the current DuckDB choice against two alternatives.

### What we did

1. Audited missing values before cleaning and saved `missing_before.png`
2. Checked for fully empty rows and duplicate rows, displaying the affected rows when present
3. Dropped unusable/duplicate columns: `vws_m_s`, `timestamp`, and `station`
4. Removed duplicate records and invalid negative pollution readings
5. Filled weather gaps using station-month medians where a local median exists
6. Saved `team_4_clean.parquet` and a stricter `team_4_clean_strict.parquet`
7. Partitioned the strict cleaned dataset into Hive-style `partitioned_clean_data/year=YYYY/month=MM/` folders
8. Benchmarked DuckDB against SQLite row-store and a document-style scan on the same analytical queries

### Cleaning evidence

| Metric | Before | After |
|---|---:|---:|
| Rows | 5,831,431 | 5,831,431 |
| Columns | 22 | 19 |
| Missing cells | 19,402,433 | 11,710,449 |
| Data quality | 84.9% | 89.4% |

The source audit found 0 duplicate rows and 0 fully empty rows in the submitted dataset. The notebook still includes explicit display/export logic, so if a rerun finds empty or duplicate rows they are shown and written under `output/week3_audit/`.

### Why DuckDB remains the best database choice

Week 2 already showed DuckDB beating SQLite by 21–43× on full-dataset analytical queries. Week 3 extends the comparison to three approaches:

| Option | Type | Result |
|---|---|---|
| DuckDB | Columnar analytical database | Best fit for parquet-backed grouped time-series analytics |
| SQLite | Row-store relational database | Simple, but slower for scans and aggregations across millions of rows |
| Document-style scan | NoSQL-style flexible records | Flexible, but inefficient for repeated structured aggregations |

DuckDB is better for this project because the workload repeatedly groups, filters, and aggregates a structured time-series dataset. It reads only the columns needed for each query, works directly with parquet, and avoids the setup overhead of a separate database server.

---

## Data Serving

**Goal:** Make the cleaned air-quality data available to users through a small API.

The project includes `api.py`, a FastAPI service backed by DuckDB. It serves the cleaned parquet outputs without loading them into a separate database server. The API looks for data in this order:

1. `partitioned_clean_data/`
2. `team_4_clean_strict.parquet`
3. `team_4_clean.parquet`
4. `team_4.parquet`

This keeps the serving layer tied to the same database choice proven above: DuckDB can query parquet directly and can use the partitioned cleaned dataset when it exists.

### Run the API

```bash
pip install -r requirements.txt
uvicorn api:app --reload
```

Then open:

```text
http://127.0.0.1:8000/docs
```

### Customer-facing endpoints

| Endpoint | Purpose |
|---|---|
| `/metadata` | Shows row count, time range, station count, and pollutant count |
| `/stations` | Lists monitoring stations |
| `/pollutants` | Lists pollutants and measurement counts |
| `/measurements` | Returns filtered readings by station, pollutant, year, and month |
| `/summary/monthly` | Monthly average/min/max for a pollutant |
| `/summary/stations` | Station ranking for a pollutant |

Example:

```text
http://127.0.0.1:8000/summary/monthly?pollutant=pm25
```

---

## Project structure

```
DT_project/
├── README.md
├── .gitignore
├── week1_ingestion.ipynb        ← ingestion, summary, partitioning
├── air_quality_duckdb.ipynb     ← data model, DuckDB, benchmarks
├── week3_cleaning_eda.ipynb     ← cleaning audit, cleaned partitioning, 3-way benchmark
├── api.py                       ← FastAPI data-serving layer
├── requirements.txt
├── output/
│   └── ingestion_summary.txt
├── team_4.parquet               ← source data (gitignored, 51.8 MB)
├── partitioned_data/            ← 24 partition files (gitignored)
└── air_quality.duckdb           ← database file (gitignored, 80.5 MB)
```

Data files are excluded from git — too large, and they're build artifacts reproducible from the notebooks.

---

## How to reproduce

```bash
pip install -r requirements.txt
```

1. Place `team_4.parquet` in the project root
2. Run `week1_ingestion.ipynb` — produces partitioned data and ingestion summary
3. Run `air_quality_duckdb.ipynb` — builds the DuckDB database and runs benchmarks
4. Run `week3_cleaning_eda.ipynb` — produces cleaned data and cleaned partitions
5. Run `uvicorn api:app --reload` — serves the data API locally

---

## Key takeaways

- Columnar storage (parquet/DuckDB) is not just theoretically faster — benchmarks show 21–43× over row-based SQLite on this dataset
- Hive-style partitioning is standard for a reason: loading one month touches one file instead of 5.8M rows
- A star schema makes queries cleaner and storage leaner — "Delhi" shouldn't be stored 5.8M times
- DuckDB can query parquet files directly without loading — useful for one-off queries where a 19-second load isn't worth it
