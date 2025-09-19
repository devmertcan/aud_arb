import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from itertools import combinations

# ---------- Load and Prepare Data ----------
csv_path = "/opt/aud_arb/out/tob_snapshots.csv"
df = pd.read_csv(csv_path)

# Parse timestamps (mixed ISO formats, UTC)
df["timestamp_iso"] = pd.to_datetime(df["timestamp_iso"], format="mixed", utc=True, errors="coerce")
df = df.dropna(subset=["timestamp_iso"])

# Group by exchange
dfs = {
    ex: g.drop_duplicates(subset=["timestamp_iso"]).set_index("timestamp_iso").resample("10s").first()
    for ex, g in df.groupby("exchange")
}

# ---------- Cross-Exchange Analysis ----------
pair_stats = []
for (ex_a, df_a), (ex_b, df_b) in combinations(dfs.items(), 2):
    merged = pd.concat({ex_a: df_a, ex_b: df_b}, axis=1).dropna()

    # Spread in both directions
    merged["spread_ab"] = merged[(ex_a, "best_bid_price")] - merged[(ex_b, "best_ask_price")]
    merged["spread_ba"] = merged[(ex_b, "best_bid_price")] - merged[(ex_a, "best_ask_price")]

    # Threshold = 0.7% of average midprice
    avg_mid = (merged[(ex_a, "best_bid_price")] + merged[(ex_b, "best_ask_price")]).mean() / 2
    threshold = 0.007 * avg_mid

    merged["success_ab"] = merged["spread_ab"] > threshold
    merged["success_ba"] = merged["spread_ba"] > threshold

    # Hourly success rates
    rate_ab = merged["success_ab"].resample("1h").mean() * 100
    rate_ba = merged["success_ba"].resample("1h").mean() * 100

    # Save summary stats
    pair_stats.append({
        "pair": f"{ex_a} sell vs {ex_b} buy",
        "success_rate_mean": rate_ab.mean(),
        "success_rate_min": rate_ab.min(),
        "success_rate_max": rate_ab.max(),
        "success_rate_std": rate_ab.std(),
        "samples": len(rate_ab.dropna())
    })
    pair_stats.append({
        "pair": f"{ex_b} sell vs {ex_a} buy",
        "success_rate_mean": rate_ba.mean(),
        "success_rate_min": rate_ba.min(),
        "success_rate_max": rate_ba.max(),
        "success_rate_std": rate_ba.std(),
        "samples": len(rate_ba.dropna())
    })

    # --- Plot one example spread for visual check ---
    plt.figure(figsize=(12,6))
    plt.plot(merged.index, merged["spread_ab"], label=f"{ex_a} bid - {ex_b} ask")
    plt.axhline(threshold, color="red", linestyle="--", label="0.7% threshold")
    plt.title(f"Cross-Exchange Spread: {ex_a} vs {ex_b}")
    plt.ylabel("Spread (AUD)")
    plt.legend()
    plt.tight_layout()
    plt.show()

# ---------- Save Pair Stats CSV ----------
pair_df = pd.DataFrame(pair_stats).sort_values("success_rate_mean", ascending=False)
pair_csv = "/opt/aud_arb/out/pair_success_rate.csv"
pair_df.to_csv(pair_csv, index=False)
print(f"[INFO] Saved pair success rates to {pair_csv}")
print(pair_df.head())

# ---------- Heatmap with Samples ----------
heatmap_df = pair_df.copy()
heatmap_df["sell"] = heatmap_df["pair"].apply(lambda x: x.split(" sell vs ")[0])
heatmap_df["buy"] = heatmap_df["pair"].apply(lambda x: x.split(" sell vs ")[1].replace(" buy",""))

matrix_rate = heatmap_df.pivot(index="sell", columns="buy", values="success_rate_mean")
matrix_samples = heatmap_df.pivot(index="sell", columns="buy", values="samples")

# Annotation: "rate% (samples)"
annot = matrix_rate.round(1).astype(str) + "% (" + matrix_samples.fillna(0).astype(int).astype(str) + ")"

plt.figure(figsize=(9,7))
sns.heatmap(matrix_rate, annot=annot, fmt="", cmap="YlGnBu", cbar_kws={'label': 'Success Rate (%)'})
plt.title("Cross-Exchange Arbitrage Success Rates (with sample counts)")
plt.ylabel("Sell Exchange")
plt.xlabel("Buy Exchange")
plt.tight_layout()
plt.show()
