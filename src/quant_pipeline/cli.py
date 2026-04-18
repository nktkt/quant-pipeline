"""Typer + Rich CLI for the quant_pipeline US-equities data pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from .config import Settings, load_settings
from .fetcher import AlpacaBarsFetcher
from .storage import BarStore
from . import updater

app = typer.Typer(no_args_is_help=True, help="US-equities data pipeline CLI.")
console = Console()
err_console = Console(stderr=True)

_TIMEFRAME_VIEWS: dict[str, str] = {
    "1Min": "bars_1min",
    "5Min": "bars_5min",
    "15Min": "bars_15min",
    "1Hour": "bars_1hour",
    "1Day": "bars_1day",
}


def _fail(msg: str) -> None:
    err_console.print(f"[bold red]Error:[/bold red] {msg}")
    raise typer.Exit(code=1)


def _parse_date(s: str, field: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        _fail(f"{field} must be YYYY-MM-DD, got {s!r}")
        raise  # unreachable


def _parse_symbols(s: str) -> list[str]:
    syms = [x.strip().upper() for x in s.split(",") if x.strip()]
    if not syms:
        _fail("SYMBOLS must be a non-empty comma-separated list")
    return syms


def _mask(secret: str) -> str:
    if not secret:
        return "<unset>"
    if len(secret) <= 4:
        return "*" * len(secret)
    return f"{secret[:2]}{'*' * (len(secret) - 4)}{secret[-2:]}"


def _counts_table(title: str, counts: dict[str, int]) -> Table:
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Symbol", style="bold")
    table.add_column("Rows", justify="right")
    total = 0
    for sym in sorted(counts):
        n = int(counts[sym])
        total += n
        table.add_row(sym, str(n))
    table.add_section()
    table.add_row("TOTAL", str(total), style="bold")
    return table


def _previous_trading_day(today: datetime) -> datetime:
    d = today - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


@app.command()
def init() -> None:
    """Create data_dir, print config (secrets masked), verify Alpaca credentials."""
    try:
        settings: Settings = load_settings()
    except Exception as e:
        _fail(f"Failed to load settings: {e}")
        return

    try:
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        settings.bars_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _fail(f"Failed to create data_dir {settings.data_dir}: {e}")
        return

    table = Table(title="Configuration", show_header=True, header_style="bold cyan")
    table.add_column("Key", style="bold")
    table.add_column("Value")
    table.add_row("data_dir", str(settings.data_dir))
    table.add_row("bars_dir", str(settings.bars_dir))
    table.add_row("duckdb_path", str(settings.duckdb_path))
    table.add_row("alpaca_paper", str(settings.alpaca_paper))
    table.add_row("alpaca_api_key", _mask(settings.alpaca_api_key))
    table.add_row("alpaca_api_secret", _mask(settings.alpaca_api_secret))
    console.print(table)

    console.print("[cyan]Verifying Alpaca credentials (AAPL 1Day, previous trading day)...[/cyan]")
    now = datetime.now(tz=timezone.utc)
    start = _previous_trading_day(now)
    end = start + timedelta(days=1)
    try:
        fetcher = AlpacaBarsFetcher(settings)
        df = fetcher.fetch_bars(["AAPL"], "1Day", start, end)
    except Exception as e:
        _fail(f"Alpaca credential check failed: {e}")
        return

    if df is None or df.empty:
        console.print(
            "[yellow]Credentials appear valid but no bars were returned "
            "for the previous trading day.[/yellow]"
        )
    else:
        console.print(
            f"[green]OK[/green] - fetched {len(df)} bar(s) for AAPL on "
            f"{start.date().isoformat()}."
        )


@app.command()
def backfill(
    symbols: str = typer.Argument(..., help="Comma-separated symbols (e.g. AAPL,MSFT,SPY)."),
    timeframe: str = typer.Option("1Day", "--timeframe", help="Bar timeframe."),
    start: str = typer.Option("2020-01-01", "--start", help="Start date YYYY-MM-DD."),
    end: Optional[str] = typer.Option(None, "--end", help="End date YYYY-MM-DD (optional)."),
) -> None:
    """Backfill bars for the given symbols over a date range."""
    syms = _parse_symbols(symbols)
    start_dt = _parse_date(start, "--start")
    end_dt = _parse_date(end, "--end") if end else None

    try:
        results = updater.backfill(syms, timeframe, start_dt, end_dt)
    except Exception as e:
        _fail(f"Backfill failed: {e}")
        return

    console.print(_counts_table(f"Backfill {timeframe}", results))


@app.command()
def update(
    symbols: str = typer.Argument(..., help="Comma-separated symbols."),
    timeframe: str = typer.Option("1Day", "--timeframe", help="Bar timeframe."),
    default_start: str = typer.Option(
        "2020-01-01",
        "--default-start",
        help="Start date used when a symbol has no stored bars yet.",
    ),
) -> None:
    """Incrementally update bars from the last stored timestamp."""
    syms = _parse_symbols(symbols)
    start_dt = _parse_date(default_start, "--default-start")

    try:
        results = updater.update(syms, timeframe, start_dt)
    except Exception as e:
        _fail(f"Update failed: {e}")
        return

    console.print(_counts_table(f"Update {timeframe}", results))


@app.command()
def info(
    timeframe: str = typer.Option("1Day", "--timeframe", help="Bar timeframe."),
) -> None:
    """Show stored symbols with row counts and date ranges."""
    if timeframe not in _TIMEFRAME_VIEWS:
        _fail(
            f"Invalid timeframe {timeframe!r}; expected one of "
            f"{sorted(_TIMEFRAME_VIEWS)}"
        )

    view = _TIMEFRAME_VIEWS[timeframe]
    settings = load_settings()
    store = BarStore(settings)

    sql = (
        f"SELECT symbol, COUNT(*) AS rows, MIN(timestamp) AS first, "
        f"MAX(timestamp) AS last FROM {view} GROUP BY symbol ORDER BY symbol"
    )
    try:
        df = store.query(sql)
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "catalog" in msg or "referenced table" in msg:
            console.print(f"[yellow]No data stored for timeframe {timeframe}.[/yellow]")
            return
        _fail(f"Query failed: {e}")
        return

    if df is None or df.empty:
        console.print(f"[yellow]No data stored for timeframe {timeframe}.[/yellow]")
        return

    table = Table(title=f"Stored bars ({timeframe})", header_style="bold cyan")
    table.add_column("Symbol", style="bold")
    table.add_column("Rows", justify="right")
    table.add_column("First")
    table.add_column("Last")
    for _, row in df.iterrows():
        table.add_row(
            str(row["symbol"]),
            str(int(row["rows"])),
            str(row["first"]),
            str(row["last"]),
        )
    console.print(table)


@app.command()
def query(sql: str = typer.Argument(..., help="SQL query to run.")) -> None:
    """Run an arbitrary SQL query against the DuckDB views."""
    settings = load_settings()
    store = BarStore(settings)
    try:
        df = store.query(sql)
    except Exception as e:
        _fail(f"Query failed: {e}")
        return

    if df is None or df.empty:
        console.print("[yellow]No rows returned.[/yellow]")
        return

    total = len(df)
    truncated = total > 50
    display = df.head(50)

    table = Table(title="Query result", header_style="bold cyan")
    for col in display.columns:
        table.add_column(str(col))
    for _, row in display.iterrows():
        table.add_row(*[str(row[c]) if not pd.isna(row[c]) else "" for c in display.columns])
    console.print(table)
    if truncated:
        console.print(
            f"[yellow]Showing 50 of {total} rows (truncated).[/yellow]"
        )
    else:
        console.print(f"[dim]{total} row(s).[/dim]")


if __name__ == "__main__":
    app()
