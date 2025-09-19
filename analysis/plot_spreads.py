import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("/opt/aud_arb/out/tob_snapshots.csv")
df["timestamp_iso"] = pd.to_datetime(df["timestamp_iso"])

ir = df[df["exchange"] == "independentreserve"].set_index("timestamp_iso")
kraken = df[df["exchange"] == "kraken"].set_index("timestamp_iso")

merged = pd.concat({"ir": ir, "kraken": kraken}, axis=1).dropna()

merged["spread_cross"] = merged[("ir", "best_bid_price")] - merged[("kraken", "best_ask_price")]

# threshold = 0.7% of average kraken ask
threshold = 0.007 * merged[("kraken", "best_ask_price")].mean()
merged["success"] = merged["spread_cross"] > threshold
rolling_rate = merged["success"].resample("1H").mean() * 100

# Plot spreads
plt.figure(figsize=(12,6))
plt.plot(merged.index, merged["spread_cross"], label="IR bid - Kraken ask")
plt.axhline(threshold, color="red", linestyle="--", label="0.7% threshold")
plt.title("Cross-Exchange Spread (IR vs Kraken)")
plt.ylabel("Spread (AUD)")
plt.legend()
plt.show()

# Plot rolling success rate
plt.figure(figsize=(12,4))
plt.plot(rolling_rate.index, rolling_rate, label="Hourly Success Rate")
plt.title("Hourly Success Rate (>0.7%)")
plt.ylabel("% of samples above threshold")
plt.ylim(0, 100)
plt.legend()
plt.show()
