import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import duckdb

    from pathlib import Path
    import sqlite3
    import gzip
    import shutil
    import tempfile

    return Path, duckdb, mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🏰 Gielinor Population Report

    ## 01 — Data Discovery

    ### Objective

    Explore the available OSRS and RS3 datasets.

    We want to identify:
    - Available source files
    - Database schemas
    - Data types
    - Observation granularity
    - Historical coverage
    - Potential data quality issues

    **No transformations at this stage.**
    """)
    return


@app.cell
def _(Path):
    PROJECT_ROOT = Path.cwd()

    RAW_DATA = PROJECT_ROOT / "data" / "raw"

    OSRS_DATA = RAW_DATA / "osrs"
    RS3_DATA = RAW_DATA / "rs3"

    assert PROJECT_ROOT.joinpath("pyproject.toml").exists(), (
        "Project root not found. Check the working directory."
    )

    PROJECT_ROOT
    return OSRS_DATA, PROJECT_ROOT, RS3_DATA


@app.cell
def _(OSRS_DATA, PROJECT_ROOT, Path, RS3_DATA, pd):
    def inventory_files(directory: Path, game: str) -> list[dict]:
        files = []

        for file_path in sorted(directory.rglob("*")):
            if file_path.is_file():
                files.append({
                    "game": game,
                    "filename": file_path.name,
                    "extension": "".join(file_path.suffixes),
                    "size_mb": round(file_path.stat().st_size / 1024**2, 2),
                    "path": str(file_path.relative_to(PROJECT_ROOT)),
                })

        return files


    source_files = (
        inventory_files(OSRS_DATA, "OSRS")
        + inventory_files(RS3_DATA, "RS3")
    )

    files_df = pd.DataFrame(source_files)

    files_df
    return


@app.cell
def _(OSRS_DATA, duckdb):
    db_path = OSRS_DATA / "osrs_data-2026-11-02.db"

    con = duckdb.connect()

    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")

    con.execute(
        f"ATTACH '{db_path.as_posix()}' AS osrs (TYPE SQLITE)"
    )
    return (con,)


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SHOW all TABLES
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        summarize osrs.players
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        summarize osrs.history_import
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT *
        FROM osrs.main.history_import
        LIMIT 20;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            stat,
            COUNT(*) AS observations,
            MIN(period_start) AS first_period,
            MAX(period_start) AS last_period,
            COUNT(DISTINCT period_start) AS distinct_periods,
            COUNT(*) - COUNT(DISTINCT period_start) AS duplicate_periods
        FROM osrs.main.history_import
        GROUP BY stat
        ORDER BY stat;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        WITH periods AS (
            SELECT DISTINCT period_start
            FROM osrs.main.history_import
        ),
        gaps AS (
            SELECT
                period_start,
                LAG(period_start) OVER (
                    ORDER BY period_start
                ) AS previous_period
            FROM periods
        )
        SELECT
            previous_period,
            period_start,
            DATE_DIFF('day', previous_period, period_start) AS gap_days
        FROM gaps
        WHERE DATE_DIFF('day', previous_period, period_start) > 7
        ORDER BY period_start;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        WITH periods AS (
            SELECT
                stat,
                period_start,
                period_end,
                LAG(period_end) OVER (
                    PARTITION BY stat
                    ORDER BY period_start
                ) AS previous_period_end
            FROM osrs.main.history_import
        )
        SELECT
            stat,
            previous_period_end,
            period_start,
            DATE_DIFF(
                'day',
                previous_period_end,
                period_start
            ) AS gap_days
        FROM periods
        WHERE previous_period_end IS NOT NULL
          AND DATE_DIFF(
              'day',
              previous_period_end,
              period_start
          ) > 0
        ORDER BY stat, period_start;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            DATE_TRUNC('month', period_start) AS month,
            stat,
            COUNT(*) AS observations,
            MIN(period_start) AS first_period,
            MAX(period_end) AS last_period,
            AVG(count) AS avg_population,
            MIN(count) AS min_population,
            MAX(count) AS max_population
        FROM osrs.main.history_import
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            DATE_TRUNC('month', period_start) AS month,
            COUNT(*) FILTER (WHERE stat = 'avg') AS avg_observations,
            COUNT(*) FILTER (WHERE stat = 'peak') AS peak_observations,
            MIN(period_start) AS first_period,
            MAX(period_end) AS last_period
        FROM osrs.main.history_import
        GROUP BY 1
        HAVING
            COUNT(*) FILTER (WHERE stat = 'avg') = 0
            OR COUNT(*) FILTER (WHERE stat = 'peak') = 0
        ORDER BY 1;
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    players table analysis
    """)
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT id) AS distinct_ids,
            COUNT(DISTINCT timestamp) AS distinct_timestamps,
            COUNT(*) - COUNT(DISTINCT timestamp) AS duplicate_timestamps,
            MIN(timestamp) AS first_observation,
            MAX(timestamp) AS last_observation,
            MIN(count) AS min_players,
            MAX(count) AS max_players,
            ROUND(AVG(count), 2) AS avg_players,
            MEDIAN(count) AS median_players
        FROM osrs.main.players;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        WITH intervals AS (
            SELECT
                timestamp,
                LAG(timestamp) OVER (
                    ORDER BY timestamp
                ) AS previous_timestamp
            FROM osrs.main.players
        )
        SELECT
            DATE_DIFF(
                'second',
                previous_timestamp,
                timestamp
            ) AS interval_seconds,
            COUNT(*) AS occurrences
        FROM intervals
        WHERE previous_timestamp IS NOT NULL
        GROUP BY 1
        ORDER BY occurrences DESC
        LIMIT 20;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        WITH intervals AS (
            SELECT
                timestamp,
                LAG(timestamp) OVER (
                    ORDER BY timestamp
                ) AS previous_timestamp
            FROM osrs.main.players
        ),
        classified AS (
            SELECT
                DATE_DIFF(
                    'second',
                    previous_timestamp,
                    timestamp
                ) AS interval_seconds
            FROM intervals
            WHERE previous_timestamp IS NOT NULL
        )
        SELECT
            CASE
                WHEN interval_seconds <= 305 THEN '01 - Normal (<= 5m05)'
                WHEN interval_seconds <= 600 THEN '02 - Short gap (5-10m)'
                WHEN interval_seconds <= 1800 THEN '03 - Medium gap (10-30m)'
                WHEN interval_seconds <= 3600 THEN '04 - Long gap (30-60m)'
                ELSE '05 - Very long gap (> 60m)'
            END AS interval_category,
            COUNT(*) AS occurrences,
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            ) AS percentage
        FROM classified
        GROUP BY 1
        ORDER BY 1;
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT
            DATE_TRUNC('day', timestamp)::DATE AS date,
            COUNT(*) AS snapshots,
            ROUND(AVG(count), 0) AS avg_players,
            MIN(count) AS min_players,
            MAX(count) AS max_players,
            MIN(timestamp) AS first_snapshot,
            MAX(timestamp) AS last_snapshot
        FROM osrs.main.players
        GROUP BY 1
        ORDER BY 1;
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(con, mo):
    _df = mo.sql(
        f"""
        describe osrs.main.scrape_events;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SUMMARIZE osrs.main.scrape_events;
        """,
        engine=con
    )
    return


if __name__ == "__main__":
    app.run()
