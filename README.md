# AUD Arbitrage Bot

A Python-based arbitrage detection bot for monitoring BTC/AUD orderbooks across multiple exchanges (Kraken, Independent Reserve).  
Milestone 1 delivers live monitoring, CSV reporting, plotting tools, and service deployment.

## Features
- REST + WebSocket connectors
- Orderbook manager with sequence gap handling
- Cross-exchange arbitrage detector
- CSV reporter for top-of-book (TOB) snapshots
- Plotting tools (spreads, success rates)
- Systemd service for 24/7 operation

## Repository Layout
- `src/` → core source code (connectors, orderbook, detector, reporter)
- `tests/` → unit tests and checklist runner
- `analysis/` → analysis and plotting scripts
- `systemd/` → example systemd service file
- `out/` → CSV outputs
- `logs/` → runtime logs

## Setup
```bash
git clone https://github.com/devmertcan/aud_arb.git
cd aud_arb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running
```bash
PYTHONPATH=src python src/main.py
```

For background service, use `systemd` (see `USAGE.md`).

## Outputs
- Logs → `/opt/aud_arb/logs/bot.log`
- CSV snapshots → `/opt/aud_arb/out/tob_snapshots.csv`
