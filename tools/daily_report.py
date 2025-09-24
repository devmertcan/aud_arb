#!/usr/bin/env python3
"""
Daily report generator for AUD arbitrage detection.

Reads the master CSV (e.g., /opt/aud_arb/out/tob_snapshots.csv),
filters by a date (default: yesterday, in UTC), computes summary stats,
creates plots, and bundles everything into a ZIP you can send to the client.

Outputs (in --out-dir):
  - daily_clean.csv
  - spread_over_time.png
  - top_pairs_bar.png
  - summary_stats.txt
  - aud_arb_daily_<YYYY-MM-DD>.zip

Usage:
  python tools/daily_report.py \
    --csv /opt/aud_arb/out/tob_snapshots.csv \
    --out-dir /opt/aud_arb/reports \
    --date 2025-09-22 \
    --threshold-after-fee 0.7
"""

import argparse
import io
import os
import sys
import zipfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import matplotlib.pyplot as plt

def parse_args():
    ap = argparse.ArgumentParser(description="Generate daily report zip from detection CSV.")
    ap.add_argument("--csv", required=True, help="Path to master CSV (e.g., /opt/aud_arb/out/tob_snapshots.csv)")
    ap.add_argument("--out-dir", required=True, help="Directory to write outputs/ZIP")
    ap.add_argument("--date", default=None, help="Target date in YYYY-MM-DD (UTC). Default: yesterday")
    ap.add_argument("--threshold-after-fee", type=float, default=0.7,
                    help="Profit threshold in percent (after fees) to count as opportunity.")
    ap.add_argument("--timezone", default="UTC",
                    help="Info only; timestamps in CSV assumed ISO UTC. Reports are labeled with this TZ name.")
    return ap.parse_args()

def _safe_parse_ts(s: str):
    try:
        # Expecting ISO UTC, eg "2025-09-19T01:40:51.521000Z"
        # pandas can parse; fall back to None on weird rows.
        return pd.to_datetime(s, utc=True)
    except Exception:
        return pd.NaT

def load_and_filter(csv_path: str, target_date_utc: datetime) -> pd.DataFrame:
    # Read CSV; columns expected from CSVReporter:
    # timestamp_iso, exchange_buy, exchange_sell, pair, best_ask_buy_ex, best_bid_sell_ex,
    # spread_aud, spread_pct, after_fee_spread_pct, notional_aud, confidence, reason
    df = pd.read_csv(csv_path)
    if "timestamp_iso" not in df.columns:
        raise RuntimeError("CSV missing 'timestamp_iso' column.")

    # Parse timestamps; drop rows with invalid timestamps
    ts = df["timestamp_iso"].astype(str).apply(_safe_parse_ts)
    df = df.assign(timestamp=ts).dropna(subset=["timestamp"])

    # Filter by UTC day
    start = target_date_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    mask = (df["timestamp"] >= start) & (df["timestamp"] < end)
    return df.loc[mask].copy()

def compute_summary(df: pd.DataFrame, threshold_after_fee: float) -> dict:
    if df.empty:
        return {
            "records": 0,
            "pairs_covered": 0,
            "exchanges_seen": 0,
            "avg_spread_aud": 0.0,
            "max_spread_aud": 0.0,
            "avg_spread_pct": 0.0,
            "max_spread_pct": 0.0,
            "opportunities_above_threshold": 0,
            "top_pairs": [],
            "top_exchanges": [],
        }

    df["spread_aud"] = pd.to_numeric(df["spread_aud"], errors="coerce")
    df["spread_pct"] = pd.to_numeric(df["spread_pct"], errors="coerce")
    df["after_fee_spread_pct"] = pd.to_numeric(df["after_fee_spread_pct"], errors="coerce")
    df["notional_aud"] = pd.to_numeric(df.get("notional_aud", 0.0), errors="coerce")

    summary = {
        "records": len(df),
        "pairs_covered": df["pair"].nunique() if "pair" in df else 0,
        "exchanges_seen": pd.unique(df[["exchange_buy", "exchange_sell"]].values.ravel("K")).size
                           if {"exchange_buy","exchange_sell"}.issubset(df.columns) else 0,
        "avg_spread_aud": float(df["spread_aud"].mean(skipna=True) or 0.0),
        "max_spread_aud": float(df["spread_aud"].max(skipna=True) or 0.0),
        "avg_spread_pct": float(df["spread_pct"].mean(skipna=True) or 0.0),
        "max_spread_pct": float(df["spread_pct"].max(skipna=True) or 0.0),
        "opportunities_above_threshold": int((df["after_fee_spread_pct"] > threshold_after_fee).sum()),
    }

    # Top pairs by count above threshold
    if {"pair", "after_fee_spread_pct"}.issubset(df.columns):
        top_pairs = (
            df[df["after_fee_spread_pct"] > threshold_after_fee]
            .groupby("pair")
            .size()
            .sort_values(ascending=False)
            .head(10)
        )
        summary["top_pairs"] = list(top_pairs.items())

    # Top exchange routes (buy->sell)
    if {"exchange_buy", "exchange_sell", "after_fee_spread_pct"}.issubset(df.columns):
        routes = (
            df[df["after_fee_spread_pct"] > threshold_after_fee]
            .assign(route=df["exchange_buy"].astype(str) + "→" + df["exchange_sell"].astype(str))
            .groupby("route")
            .size()
            .sort_values(ascending=False)
            .head(10)
        )
        summary["top_exchanges"] = list(routes.items())

    return summary

def write_summary_txt(path: str, target_date: str, tz_label: str, threshold: float, summary: dict):
    with open(path, "w") as f:
        f.write(f"Detection Summary for {target_date} ({tz_label})\n")
        f.write("=" * 48 + "\n\n")
        f.write(f"Records captured: {summary['records']}\n")
        f.write(f"Pairs covered: {summary['pairs_covered']}\n")
        f.write(f"Distinct exchanges seen: {summary['exchanges_seen']}\n\n")
        f.write(f"Average spread: {summary['avg_spread_aud']:.2f} AUD ({summary['avg_spread_pct']:.3f}%)\n")
        f.write(f"Max spread: {summary['max_spread_aud']:.2f} AUD ({summary['max_spread_pct']:.3f}%)\n")
        f.write(f"Opportunities above {threshold:.2f}% after fees: {summary['opportunities_above_threshold']}\n\n")

        if summary.get("top_pairs"):
            f.write("Top pairs by count above threshold:\n")
            for pair, cnt in summary["top_pairs"]:
                f.write(f"  - {pair}: {cnt}\n")
            f.write("\n")

        if summary.get("top_exchanges"):
            f.write("Top exchange routes (buy→sell) above threshold:\n")
            for route, cnt in summary["top_exchanges"]:
                f.write(f"  - {route}: {cnt}\n")

def plot_spread_over_time(df: pd.DataFrame, out_png: str):
    if df.empty:
        # create an empty placeholder chart
        plt.figure(figsize=(11,5))
        plt.title("Spread Over Time (no data)")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Spread (AUD)")
        plt.tight_layout()
        plt.savefig(out_png)
        plt.close()
        return

    plt.figure(figsize=(11,5))
    # plot absolute spread; use a light marker density
    plt.plot(df["timestamp"], df["spread_aud"], marker=".", linestyle="-", alpha=0.5)
    plt.title("Spread Over Time (absolute AUD)")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Spread (AUD)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def plot_top_pairs_bar(df: pd.DataFrame, threshold_after_fee: float, out_png: str):
    filt = df[df["after_fee_spread_pct"] > threshold_after_fee]
    if filt.empty or "pair" not in filt.columns:
        # placeholder
        plt.figure(figsize=(10,5))
        plt.title("Top Pairs Above Threshold (no data)")
        plt.tight_layout()
        plt.savefig(out_png)
        plt.close()
        return

    counts = filt.groupby("pair").size().sort_values(ascending=False).head(10)
    plt.figure(figsize=(10,5))
    counts.plot(kind="bar")
    plt.title(f"Top Pairs by Count (> {threshold_after_fee:.2f}% after-fee)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    # Determine date (UTC)
    if args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print("Invalid --date format, use YYYY-MM-DD", file=sys.stderr)
            sys.exit(2)
    else:
        # default to yesterday UTC
        target = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Load + filter
    df = load_and_filter(args.csv, target)

    # Write daily clean CSV
    daily_csv = os.path.join(args.out_dir, "daily_clean.csv")
    cols = [
        "timestamp_iso","exchange_buy","exchange_sell","pair",
        "best_ask_buy_ex","best_bid_sell_ex","spread_aud","spread_pct",
        "after_fee_spread_pct","notional_aud","confidence","reason"
    ]
    existing_cols = [c for c in cols if c in df.columns]
    df[existing_cols].to_csv(daily_csv, index=False)

    # Stats + summary
    summary = compute_summary(df, args.threshold_after_fee)
    summary_txt = os.path.join(args.out_dir, "summary_stats.txt")
    label_date = target.strftime("%Y-%m-%d")
    write_summary_txt(summary_txt, label_date, args.timezone, args.threshold_after_fee, summary)

    # Plots
    # Ensure sorted by time for plotting
    df_sorted = df.sort_values("timestamp")
    spread_png = os.path.join(args.out_dir, "spread_over_time.png")
    plot_spread_over_time(df_sorted, spread_png)

    top_pairs_png = os.path.join(args.out_dir, "top_pairs_bar.png")
    plot_top_pairs_bar(df_sorted, args.threshold_after_fee, top_pairs_png)

    # ZIP bundle
    zip_name = f"aud_arb_daily_{label_date}.zip"
    zip_path = os.path.join(args.out_dir, zip_name)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(daily_csv, arcname="daily_clean.csv")
        z.write(spread_png, arcname="spread_over_time.png")
        z.write(top_pairs_png, arcname="top_pairs_bar.png")
        z.write(summary_txt, arcname="summary_stats.txt")

    print(f"[OK] Daily report written: {zip_path}")

if __name__ == "__main__":
    main()
