#!/usr/bin/env python3
"""
Daily arbitrage report for aud_arb.

Reads /opt/aud_arb/out/tob_snapshots.csv (or a custom path) and produces:
- summary_<DATE>.csv    (key metrics and top lists in tidy format)
- plot_timeseries_<DATE>.png  (minute-averaged net spread over time; all symbols)
- plot_hist_<DATE>.png        (distribution of net spread; meets_threshold=1)
- plot_top_pairs_<DATE>.png   (top symbols by opp count; meets_threshold=1)

Usage examples:
  python tools/daily_report.py --csv ./out/tob_snapshots.csv --date 2025-09-26
  python tools/daily_report.py --csv ./out/tob_snapshots.csv --start 2025-09-24 --end 2025-09-26
  python tools/daily_report.py --outdir ./out/reports
"""

import argparse
import os
from pathlib import Path
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt


def parse_args():
    p = argparse.ArgumentParser(description="Summarize daily AUD arb snapshots and plot quick charts.")
    p.add_argument("--csv", default="./out/tob_snapshots.csv", help="Path to tob_snapshots.csv")
    p.add_argument("--outdir", default="./out/reports", help="Directory to write summary & plots")
    # choose either --date (single UTC day) OR [--start, --end] inclusive bounds
    p.add_argument("--date", help="UTC date (YYYY-MM-DD) to include")
    p.add_argument("--start", help="UTC start date (YYYY-MM-DD), inclusive")
    p.add_argument("--end", help="UTC end date (YYYY-MM-DD), inclusive")
    p.add_argument("--topn", type=int, default=10, help="How many top pairs/routes to include in plots")
    p.add_argument("--resample", default="1min", help="Timeseries resample freq (e.g., 30s, 1min, 5min)")
    return p.parse_args()


NUMERIC_COLS = [
    "timestamp",
    "buy_ask",
    "sell_bid",
    "gross_spread_bps",
    "fees_bps",
    "slippage_bps",
    "net_spread_bps",
    "meets_threshold",
]


def load_df(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    # be robust to partial writes
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_csv(csv_path)
    expected_cols = [
        "ts_iso","timestamp","symbol",
        "buy_ex","buy_ask","sell_ex","sell_bid",
        "gross_spread_bps","fees_bps","slippage_bps","net_spread_bps","meets_threshold"
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        print(f"ERROR: CSV missing expected columns: {missing}", file=sys.stderr)
        sys.exit(1)

    # parse times
    df["ts_iso"] = pd.to_datetime(df["ts_iso"], errors="coerce", utc=True)
    # coerce numerics
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # drop obviously broken rows
    df = df.dropna(subset=["ts_iso", "symbol", "buy_ex", "sell_ex", "net_spread_bps"])
    return df


def filter_date(df: pd.DataFrame, date=None, start=None, end=None) -> pd.DataFrame:
    if date:
        d0 = pd.Timestamp(date).tz_localize("UTC")
        d1 = d0 + pd.Timedelta(days=1)
        m = (df["ts_iso"] >= d0) & (df["ts_iso"] < d1)
        return df.loc[m].copy()
    if start or end:
        if start:
            s0 = pd.Timestamp(start).tz_localize("UTC")
        else:
            s0 = df["ts_iso"].min()
        if end:
            e1 = pd.Timestamp(end).tz_localize("UTC") + pd.Timedelta(days=1)
        else:
            e1 = df["ts_iso"].max() + pd.Timedelta(seconds=1)
        m = (df["ts_iso"] >= s0) & (df["ts_iso"] < e1)
        return df.loc[m].copy()
    # default: today UTC
    today = pd.Timestamp.utcnow().normalize().tz_localize("UTC")
    tomorrow = today + pd.Timedelta(days=1)
    m = (df["ts_iso"] >= today) & (df["ts_iso"] < tomorrow)
    return df.loc[m].copy()


def summarize(df: pd.DataFrame) -> dict:
    total = len(df)
    opps = int((df["meets_threshold"] == 1).sum())
    opp_rate = (opps / total * 100.0) if total else 0.0

    avg_gross = df["gross_spread_bps"].mean() if total else np.nan
    avg_net = df["net_spread_bps"].mean() if total else np.nan
    med_net = df["net_spread_bps"].median() if total else np.nan
    max_row = df.loc[df["net_spread_bps"].idxmax()] if total else None

    # top symbols by opp count (meets=1)
    df_meets = df[df["meets_threshold"] == 1]
    top_pairs = (
        df_meets.groupby("symbol", as_index=False)
        .size()
        .sort_values("size", ascending=False)
    )

    # top symbols by mean net bps (min 3 events)
    top_pairs_by_net = (
        df_meets.groupby("symbol", as_index=False)["net_spread_bps"]
        .agg(["count", "mean", "median", "max"])
        .reset_index()
        .rename(columns={"count": "events", "mean": "mean_net_bps", "median": "median_net_bps", "max": "max_net_bps"})
        .query("events >= 3")
        .sort_values("mean_net_bps", ascending=False)
    )

    # top routes (buy_ex -> sell_ex) by opp frequency
    if not df_meets.empty:
        df_meets["route"] = df_meets["buy_ex"] + "→" + df_meets["sell_ex"]
        top_routes = (
            df_meets.groupby("route", as_index=False)
            .size()
            .sort_values("size", ascending=False)
        )
    else:
        top_routes = pd.DataFrame(columns=["route", "size"])

    summary = {
        "total_rows": total,
        "opportunities": opps,
        "opportunity_rate_pct": round(opp_rate, 2),
        "avg_gross_bps": None if pd.isna(avg_gross) else round(float(avg_gross), 2),
        "avg_net_bps": None if pd.isna(avg_net) else round(float(avg_net), 2),
        "median_net_bps": None if pd.isna(med_net) else round(float(med_net), 2),
        "max_net_row": max_row.to_dict() if max_row is not None else {},
        "top_pairs_by_count": top_pairs,
        "top_pairs_by_mean_net": top_pairs_by_net,
        "top_routes_by_count": top_routes,
    }
    return summary


def write_summary_csv(summary: dict, out_csv: Path, topn: int):
    rows = []
    rows.append({"metric": "total_rows", "value": summary["total_rows"]})
    rows.append({"metric": "opportunities", "value": summary["opportunities"]})
    rows.append({"metric": "opportunity_rate_pct", "value": summary["opportunity_rate_pct"]})
    rows.append({"metric": "avg_gross_bps", "value": summary["avg_gross_bps"]})
    rows.append({"metric": "avg_net_bps", "value": summary["avg_net_bps"]})
    rows.append({"metric": "median_net_bps", "value": summary["median_net_bps"]})

    # flatten max row key bits
    max_row = summary.get("max_net_row", {}) or {}
    for k in ("ts_iso","symbol","buy_ex","sell_ex","net_spread_bps","gross_spread_bps","fees_bps"):
        if k in max_row:
            rows.append({"metric": f"max_net_{k}", "value": max_row[k]})

    # Top lists
    tp = summary["top_pairs_by_count"].head(topn).copy()
    tp["metric"] = "top_pair_by_count"
    tp.rename(columns={"symbol": "key", "size": "value"}, inplace=True)
    rows += tp[["metric", "key", "value"]].to_dict(orient="records")

    tpn = summary["top_pairs_by_mean_net"].head(topn).copy()
    tpn["metric"] = "top_pair_by_mean_net"
    tpn.rename(columns={"symbol": "key", "mean_net_bps": "value"}, inplace=True)
    rows += tpn[["metric", "key", "value"]].to_dict(orient="records")

    tr = summary["top_routes_by_count"].head(topn).copy()
    tr["metric"] = "top_route_by_count"
    tr.rename(columns={"route": "key", "size": "value"}, inplace=True)
    rows += tr[["metric", "key", "value"]].to_dict(orient="records")

    pd.DataFrame(rows).to_csv(out_csv, index=False)


def plot_timeseries(df: pd.DataFrame, out_png: Path, resample="1min"):
    if df.empty:
        return
    ts = df.copy()
    ts = ts.set_index("ts_iso").sort_index()
    # minute average across all symbols; you can adjust to show only meets=1 or a specific symbol
    agg = ts["net_spread_bps"].resample(resample).mean()
    if agg.dropna().empty:
        return
    plt.figure(figsize=(10, 4))
    agg.plot()  # no explicit color/style
    plt.title(f"Average Net Spread ({resample})")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Net Spread (bps)")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


def plot_histogram(df_meets: pd.DataFrame, out_png: Path):
    if df_meets.empty:
        return
    plt.figure(figsize=(8, 4))
    vals = df_meets["net_spread_bps"].dropna().values
    if len(vals) == 0:
        return
    plt.hist(vals, bins=40)
    plt.title("Net Spread Distribution (meets_threshold=1)")
    plt.xlabel("Net Spread (bps)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


def plot_top_pairs(df_meets: pd.DataFrame, out_png: Path, topn: int):
    if df_meets.empty:
        return
    counts = (
        df_meets.groupby("symbol", as_index=False)
        .size()
        .sort_values("size", ascending=False)
        .head(topn)
    )
    if counts.empty:
        return
    plt.figure(figsize=(10, 5))
    plt.bar(counts["symbol"], counts["size"])
    plt.title(f"Top {topn} Pairs by Opportunity Count (meets_threshold=1)")
    plt.xlabel("Symbol")
    plt.ylabel("Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()


def main():
    args = parse_args()
    csv_path = Path(args.csv).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_df(csv_path)
    df_day = filter_date(df, date=args.date, start=args.start, end=args.end)

    if df_day.empty:
        print("No rows matched the selected date/range. Nothing to summarize.")
        return

    # build file tag (date or range)
    if args.date:
        tag = args.date
    elif args.start or args.end:
        tag = f"{args.start or df_day['ts_iso'].min().date()}_to_{args.end or df_day['ts_iso'].max().date()}"
    else:
        tag = str(pd.Timestamp.utcnow().date())

    # summary
    summary = summarize(df_day)
    out_csv = outdir / f"summary_{tag}.csv"
    write_summary_csv(summary, out_csv, args.topn)

    # plots
    ts_png = outdir / f"plot_timeseries_{tag}.png"
    plot_timeseries(df_day, ts_png, resample=args.resample)

    meets = df_day[df_day["meets_threshold"] == 1]
    hist_png = outdir / f"plot_hist_{tag}.png"
    plot_histogram(meets, hist_png)

    top_png = outdir / f"plot_top_pairs_{tag}.png"
    plot_top_pairs(meets, top_png, args.topn)

    print(f"✅ Wrote summary: {out_csv}")
    if ts_png.exists(): print(f"🖼  Timeseries: {ts_png}")
    if hist_png.exists(): print(f"🖼  Histogram:  {hist_png}")
    if top_png.exists(): print(f"🖼  Top pairs:  {top_png}")


if __name__ == "__main__":
    main()
