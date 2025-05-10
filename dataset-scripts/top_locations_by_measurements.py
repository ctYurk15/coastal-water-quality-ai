import pandas as pd

# === Path to coastal water data ===
CSV_PATH = "datasets/coastal_water_only.csv"

# === Load only necessary column ===
print("Loading data...")
df = pd.read_csv(CSV_PATH, usecols=["monitoringSiteIdentifier"], low_memory=False)

# === Count number of rows per location ===
print("Counting measurements per location...")
counts = (
    df["monitoringSiteIdentifier"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "monitoringSiteIdentifier", "monitoringSiteIdentifier": "measurement_count"})
)

# === Display top 20 ===
print("Top 20 locations by number of measurements:")
print(counts.to_string(index=False))
