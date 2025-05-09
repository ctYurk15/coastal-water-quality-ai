import pandas as pd
import os

# === Step 1: Ask for user input ===
location_id = input("Enter monitoring site ID (default = EESJA0094000): ").strip()
if not location_id:
    location_id = "EESJA0094000"

# === File paths ===
CSV_PATH = os.path.join("datasets", "coastal_water_only.csv")
OUTPUT_DIR = os.path.join("timeseries", location_id)
os.makedirs(OUTPUT_DIR, exist_ok=True)

DATE_COLUMN = "phenomenonTimeSamplingDate"

# === Load and preprocess ===
print(f"Loading data for location: {location_id}")
df = pd.read_csv(CSV_PATH, low_memory=False)

# Convert date column
df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="%Y%m%d", errors="coerce")
df = df.dropna(subset=[DATE_COLUMN])

# Convert values to float
df["resultObservedValue"] = pd.to_numeric(df["resultObservedValue"], errors="coerce")
df = df.dropna(subset=["resultObservedValue"])

# Filter by location
df = df[df["monitoringSiteIdentifier"] == location_id]

# Check if any data remains
if df.empty:
    print(f"No data found for location ID: {location_id}")
    exit()

# Get unique parameters
parameters = df["observedPropertyDeterminandLabel"].dropna().unique()

# Generate one time series per parameter
for param in parameters:
    param_df = df[df["observedPropertyDeterminandLabel"] == param]
    ts = (
        param_df
        .groupby(DATE_COLUMN)["resultObservedValue"]
        .mean()
        .reset_index()
        .rename(columns={"resultObservedValue": "value"})
    )

    # Create safe filename
    safe_name = param.replace('/', '_').replace(' ', '_').replace(':', '_')
    output_path = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")

    ts.to_csv(output_path, index=False)
    print(f"Saved: {output_path} ({len(ts)} rows)")

print(f"All time series saved in: {OUTPUT_DIR}")
