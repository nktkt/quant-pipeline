# quant-pipeline

US-equities market data pipeline for live quantitative trading.
Fetches bars from Alpaca, stores them as partitioned Parquet, and exposes a DuckDB query layer — all behind a single `qpipe` CLI.

## Features

- **Alpaca Market Data** — daily, hourly, and minute bars via `alpaca-py`.
- **Parquet storage** — Hive-partitioned by `timeframe / symbol / year`, zstd compression.
- **DuckDB query layer** — ad-hoc SQL over the Parquet store with persistent metadata.
- **Idempotent upsert** — safe to re-run; duplicates are deduped on `timestamp`.
- **Incremental updates** — fetches only new bars since the last stored timestamp.
- **Typer + Rich CLI** — ergonomic commands, pretty tables, masked secrets.

## Requirements

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- An Alpaca account (paper is fine) — [alpaca.markets](https://alpaca.markets)

## Quickstart

```bash
# 1. Install
uv sync

# 2. Configure credentials
cp .env.example .env
# edit .env and set ALPACA_API_KEY / ALPACA_API_SECRET

# 3. Initialize data directory and verify credentials
uv run qpipe init

# 4. Backfill historical daily bars
uv run qpipe backfill AAPL,MSFT,SPY --timeframe 1Day --start 2020-01-01

# 5. Incremental update (run daily)
uv run qpipe update AAPL,MSFT,SPY --timeframe 1Day --default-start 2020-01-01

# 6. Inspect what's stored
uv run qpipe info --timeframe 1Day

# 7. Run SQL
uv run qpipe query "SELECT symbol, COUNT(*) AS bars, AVG(close) AS avg_close
                    FROM bars_1day GROUP BY symbol ORDER BY symbol"
```

## Supported timeframes

`1Min`, `5Min`, `15Min`, `1Hour`, `1Day`

DuckDB views: `bars_1min`, `bars_5min`, `bars_15min`, `bars_1hour`, `bars_1day`.

## Data schema

Tidy DataFrame / Parquet columns:

| column        | type              | notes                        |
| ------------- | ----------------- | ---------------------------- |
| `symbol`      | string            | partition key                |
| `timestamp`   | timestamp (UTC)   | partition key (via `year=`)  |
| `open`        | float64           |                              |
| `high`        | float64           |                              |
| `low`         | float64           |                              |
| `close`       | float64           |                              |
| `volume`      | int64             |                              |
| `trade_count` | int64             |                              |
| `vwap`        | float64           |                              |

On disk:

```
data/
├── quant.duckdb
└── bars/
    └── 1Day/
        ├── AAPL/year=2020/bars.parquet
        ├── AAPL/year=2021/bars.parquet
        └── MSFT/year=2020/bars.parquet
```

## Project layout

```
src/quant_pipeline/
├── config.py     # pydantic-settings (loads .env)
├── fetcher.py    # Alpaca bars fetcher
├── storage.py    # Parquet + DuckDB layer
├── updater.py    # backfill / incremental update
└── cli.py        # Typer entry point (qpipe)
```

## Configuration

Environment variables (see `.env.example`):

| variable             | default        | description                                    |
| -------------------- | -------------- | ---------------------------------------------- |
| `ALPACA_API_KEY`     | _(required)_   | Alpaca API key                                 |
| `ALPACA_API_SECRET`  | _(required)_   | Alpaca API secret                              |
| `ALPACA_PAPER`       | `true`         | Paper vs. live trading endpoint (data shared) |
| `QPIPE_DATA_DIR`     | `./data`       | Root data directory                            |

## Roadmap

- [ ] Backtest engine (vectorbt-based)
- [ ] Signal generation module
- [ ] Order execution engine (Alpaca trading API)
- [ ] Risk management (position sizing, max drawdown, kill switch)
- [ ] Monitoring & notifications (Slack/Discord)

## License

Private — not for redistribution.
