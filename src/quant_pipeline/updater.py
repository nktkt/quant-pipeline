"""Incremental update and backfill logic for OHLCV bars."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from .config import Settings, load_settings
from .fetcher import AlpacaBarsFetcher
from .storage import BarStore

_TIMEFRAME_DELTAS: dict[str, timedelta] = {
    "1Min": timedelta(minutes=1),
    "5Min": timedelta(minutes=5),
    "15Min": timedelta(minutes=15),
    "1Hour": timedelta(hours=1),
    "1Day": timedelta(days=1),
}


def timeframe_delta(tf: str) -> timedelta:
    """Return the ``timedelta`` corresponding to a timeframe string."""
    if tf not in _TIMEFRAME_DELTAS:
        raise ValueError(
            f"Invalid timeframe {tf!r}; expected one of {sorted(_TIMEFRAME_DELTAS)}"
        )
    return _TIMEFRAME_DELTAS[tf]


def _build(settings: Settings | None) -> tuple[AlpacaBarsFetcher, BarStore]:
    s = settings if settings is not None else load_settings()
    return AlpacaBarsFetcher(s), BarStore(s)


def _count_rows_by_symbol(df, symbols: list[str]) -> dict[str, int]:
    counts = {sym: 0 for sym in symbols}
    if df is None or df.empty:
        return counts
    grouped = df.groupby("symbol").size()
    for sym, n in grouped.items():
        if sym in counts:
            counts[sym] = int(n)
    return counts


def update(
    symbols: list[str],
    timeframe: str,
    default_start: datetime,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Incrementally fetch and store new bars for each symbol."""
    if not symbols:
        return {}

    fetcher, store = _build(settings)
    now = datetime.now(tz=timezone.utc)
    step = timeframe_delta(timeframe)

    # Group symbols by effective start time to share fetch calls.
    groups: dict[datetime, list[str]] = defaultdict(list)
    for sym in symbols:
        last = store.last_timestamp(sym, timeframe)
        start = default_start if last is None else last + step
        groups[start].append(sym)

    results: dict[str, int] = {sym: 0 for sym in symbols}
    for start, group_syms in groups.items():
        if start >= now:
            continue
        df = fetcher.fetch_bars(group_syms, timeframe, start, now)
        if df is None or df.empty:
            continue
        store.save(df, timeframe)
        for sym, n in _count_rows_by_symbol(df, group_syms).items():
            results[sym] = n
    return results


def backfill(
    symbols: list[str],
    timeframe: str,
    start: datetime,
    end: datetime | None = None,
    settings: Settings | None = None,
) -> dict[str, int]:
    """Unconditionally fetch and store bars for each symbol in the given range."""
    if not symbols:
        return {}

    fetcher, store = _build(settings)
    df = fetcher.fetch_bars(symbols, timeframe, start, end)
    results = {sym: 0 for sym in symbols}
    if df is None or df.empty:
        return results
    store.save(df, timeframe)
    for sym, n in _count_rows_by_symbol(df, symbols).items():
        results[sym] = n
    return results
