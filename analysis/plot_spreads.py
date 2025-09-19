import pandas as pd
import matplotlib.pyplot as plt

# Load TOB snapshots
csv_path = "/opt/aud_arb/out/tob_snapshots.csv"
df = pd.read_csv(csv_path)

# Parse timestamps (handles mixed ISO formats)
df["timestamp_iso"] = pd.to_datetime(df["timestamp_iso"], format="mixed", utc=True, errors="coerce")
df = df.dropna(subset=["timestamp_iso"])

# Split by exchange
ir = df[df["exchange"] == "independentreserve"].copy()
kraken = df[df["exchange"] == "kraken"].copy()

# Drop duplicates and resample to consistent intervals
ir = ir.drop_duplicates(subset=["timestamp_iso"]).set_index("timestamp_iso").resample("10s").first()
kraken = kraken.drop_duplicates(subset=["timestamp_iso"]).set_index("timestamp_iso").resample("10s").first()

# Align and drop missing rows
merged = pd.concat({"ir": ir, "kraken": kraken}, axis=1).dropna()

# Compute cross-exchange spread (IR bid - Kraken ask)
merged["spread_cross"] = merged[("ir", "best_bid_price")] - merged[("kraken", "best_ask_price")]

# Threshold = 0.7% of avg Kraken ask
threshold = 0.007 * merged[("kraken", "best_ask_price")].mean()
merged["success"] = merged["spread_cross"] > threshold

# Rolling hourly success rate
rolling_rate = merged["success"].resample("1h").mean() * 100

# --------- SAVE TO CSV ---------
success_csv = "/opt/aud_arb/out/success_rate.csv"
rolling_rate.to_csv(success_csv, header=["success_rate_pct"])
print(f"[INFO] Saved success rate to {success_csv}")

# --------- PLOTS ---------
plt.figure(figsize=(12,6))
plt.plot(merged.index, merged["spread_cross"], label="IR bid - Kraken ask")
plt.axhline(threshold, color="red", linestyle="--", label="0.7% threshold")
plt.title("Cross-Exchange Spread (Independent Reserve vs Kraken)")
plt.ylabel("Spread (AUD)")
plt.legend()
plt.tight_layout()
plt.show()

plt.figure(figsize=(12,4))
plt.plot(rolling_rate.index, rolling_rate, label="Hourly Success Rate (>0.7%)")
plt.title("Hourly Success Rate")
plt.ylabel("% of samples above threshold")
plt.ylim(0, 100)
plt.legend()
plt.tight_layout()
plt.show()
