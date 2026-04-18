"""Alpaca market-data fetcher for historical stock bars."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from .config import Settings

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

_TIMEFRAME_MAP: dict[str, tuple[int, TimeFrameUnit]] = {
    "1Min": (1, TimeFrameUnit.Minute),
    "5Min": (5, TimeFrameUnit.Minute),
    "15Min": (15, TimeFrameUnit.Minute),
    "1Hour": (1, TimeFrameUnit.Hour),
    "1Day": (1, TimeFrameUnit.Day),
}

_SYMBOL_BATCH_SIZE = 200


def parse_timeframe(s: str) -> TimeFrame:
    """Map a string timeframe to an alpaca-py ``TimeFrame`` instance."""
    if s not in _TIMEFRAME_MAP:
        raise ValueError(
            f"Invalid timeframe {s!r}; expected one of {sorted(_TIMEFRAME_MAP)}"
        )
    amount, unit = _TIMEFRAME_MAP[s]
    return TimeFrame(amount, unit)


def _empty_frame() -> pd.DataFrame:
    df = pd.DataFrame({col: pd.Series(dtype="object") for col in _BAR_COLUMNS})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


class AlpacaBarsFetcher:
    """Fetches historical OHLCV bars from Alpaca into tidy DataFrames."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = StockHistoricalDataClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_api_secret,
        )

    def fetch_bars(
        self,
        symbols: list[str],
        timeframe: str,
        start: datetime,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            raise ValueError("symbols must be a non-empty list")

        tf = parse_timeframe(timeframe)
        frames: list[pd.DataFrame] = []

        for i in range(0, len(symbols), _SYMBOL_BATCH_SIZE):
            batch = symbols[i : i + _SYMBOL_BATCH_SIZE]
            request = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=tf,
                start=start,
                end=end,
            )
            response = self._client.get_stock_bars(request)
            df = self._response_to_frame(response)
            if not df.empty:
                frames.append(df)

        if not frames:
            return _empty_frame()

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        return result[_BAR_COLUMNS]

    @staticmethod
    def _response_to_frame(response) -> pd.DataFrame:
        df = getattr(response, "df", None)
        if df is None or df.empty:
            return _empty_frame()

        df = df.reset_index()
        # Normalize column names; alpaca-py uses MultiIndex (symbol, timestamp).
        rename = {}
        if "symbol" not in df.columns and "S" in df.columns:
            rename["S"] = "symbol"
        if "timestamp" not in df.columns and "t" in df.columns:
            rename["t"] = "timestamp"
        if rename:
            df = df.rename(columns=rename)

        for col in _BAR_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df[_BAR_COLUMNS]
