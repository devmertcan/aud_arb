# Usage Guide: AUD Arbitrage Bot

This guide explains how to run, monitor, and configure the arbitrage bot.

## Running the Bot
Activate virtual environment and start:
```bash
source /opt/aud_arb/.venv/bin/activate
PYTHONPATH=src python src/main.py
```

For long-term operation, install as a systemd service.

## Systemd Setup
Create `/etc/systemd/system/aud_arb.service`:
```
[Unit]
Description=AUD Arbitrage Bot
After=network.target

[Service]
WorkingDirectory=/opt/aud_arb
ExecStart=/opt/aud_arb/.venv/bin/python -u src/main.py
Restart=always
User=mertcan
StandardOutput=append:/opt/aud_arb/logs/bot.log
StandardError=append:/opt/aud_arb/logs/bot.log

[Install]
WantedBy=multi-user.target
```

Reload and enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable aud_arb
sudo systemctl start aud_arb
```

Check status:
```bash
systemctl status aud_arb
```

## Logs & Data
- Logs: `/opt/aud_arb/logs/bot.log`  
- CSV snapshots: `/opt/aud_arb/out/tob_snapshots.csv`  
- Pair success analysis: `/opt/aud_arb/out/pair_success_rate.csv`  

## Parameters
- `threshold`: arbitrage spread % (default 0.7%)
- Max trades, stop-loss, min profit threshold → configured later in trading phase
- Edit `main.py` or environment variables to tune.

