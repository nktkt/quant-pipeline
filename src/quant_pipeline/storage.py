"""Parquet storage and DuckDB query layer for OHLCV bars."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings

_TIMEFRAMES: dict[str, str] = {
    "1Min": "bars_1min",
    "5Min": "bars_5min",
    "15Min": "bars_15min",
    "1Hour": "bars_1hour",
    "1Day": "bars_1day",
}

_BAR_COLUMNS = [
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
]


class BarStore:
    """Stores bars as partitioned Parquet, queryable via DuckDB."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._bars_dir = settings.bars_dir
        self._duckdb_path = settings.duckdb_path

    # ---------- Paths ----------

    def _timeframe_dir(self, timeframe: str) -> Path:
        return self._bars_dir / timeframe

    def _partition_path(self, timeframe: str, symbol: str, year: int) -> Path:
        return (
            self._timeframe_dir(timeframe)
            / symbol
            / f"year={year}"
            / "bars.parquet"
        )

    def _timeframe_glob(self, timeframe: str) -> str:
        return str(self._timeframe_dir(timeframe) / "**" / "bars.parquet")

    # ---------- Save ----------

    def save(self, df: pd.DataFrame, timeframe: str) -> int:
        """Idempotent upsert of bars into per-(symbol, year) Parquet files."""
        if df is None or df.empty:
            return 0

        if timeframe not in _TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAMES)}"
            )

        df = df[_BAR_COLUMNS].copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["_year"] = df["timestamp"].dt.year

        total_written = 0
        for (symbol, year), part in df.groupby(["symbol", "_year"], sort=False):
            part = part.drop(columns="_year")
            path = self._partition_path(timeframe, str(symbol), int(year))
            path.parent.mkdir(parents=True, exist_ok=True)

            if path.exists():
                existing = pq.read_table(path).to_pandas()
                combined = pd.concat([existing, part], ignore_index=True)
            else:
                combined = part

            combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True)
            combined = (
                combined.drop_duplicates(subset="timestamp", keep="last")
                .sort_values("timestamp")
                .reset_index(drop=True)
            )
            combined = combined[_BAR_COLUMNS]

            table = pa.Table.from_pandas(combined, preserve_index=False)
            pq.write_table(table, path, compression="zstd")
            total_written += len(combined)

        return total_written

    # ---------- Query helpers ----------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        self._duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self._duckdb_path))
        for timeframe, view in _TIMEFRAMES.items():
            tf_dir = self._timeframe_dir(timeframe)
            if not tf_dir.exists():
                continue
            glob = self._timeframe_glob(timeframe)
            con.execute(
                f"CREATE OR REPLACE VIEW {view} AS "
                f"SELECT * FROM read_parquet(?, hive_partitioning=true)",
                [glob],
            )
        return con

    def last_timestamp(self, symbol: str, timeframe: str) -> datetime | None:
        """Return the latest stored timestamp for a (symbol, timeframe), else None."""
        if timeframe not in _TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAMES)}"
            )
        symbol_dir = self._timeframe_dir(timeframe) / symbol
        if not symbol_dir.exists():
            return None

        glob = str(symbol_dir / "**" / "bars.parquet")
        con = self._connect()
        try:
            result = con.execute(
                "SELECT max(timestamp) FROM read_parquet(?, hive_partitioning=true) "
                "WHERE symbol = ?",
                [glob, symbol],
            ).fetchone()
        finally:
            con.close()

        if not result or result[0] is None:
            return None
        ts = result[0]
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime()
        return ts

    def query(self, sql: str) -> pd.DataFrame:
        """Run an arbitrary SQL statement with per-timeframe views registered."""
        con = self._connect()
        try:
            return con.execute(sql).fetch_df()
        finally:
            con.close()

    def symbols(self, timeframe: str) -> list[str]:
        """List stored symbols for a timeframe by scanning the directory layout."""
        if timeframe not in _TIMEFRAMES:
            raise ValueError(
                f"Invalid timeframe {timeframe!r}; expected one of {sorted(_TIMEFRAMES)}"
            )
        tf_dir = self._timeframe_dir(timeframe)
        if not tf_dir.exists():
            return []
        return sorted(p.name for p in tf_dir.iterdir() if p.is_dir())
