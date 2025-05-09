import pandas as pd

# === Config ===
CSV_PATH = "datasets/coastal_water_only.csv"  # Path to your CSV
TARGET_PARAMETER = "PCB 126 (3,3’,4,4’,5-pentachlorobiphenyl)"  # Parameter to analyze
TARGET_LOCATION = "EESJA0094000"  # Location identifier to filter
DATE_COLUMN = "phenomenonTimeSamplingDate"

# === Step 1: Load data ===
print("Loading data...")
df = pd.read_csv(CSV_PATH, low_memory=False)

# === Step 2: Convert date column to datetime ===
print("Converting date column to datetime...")
df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="%Y%m%d", errors="coerce")
df = df.dropna(subset=[DATE_COLUMN])

# === Step 3: Convert resultObservedValue to float ===
print("Converting resultObservedValue to float...")
df["resultObservedValue"] = pd.to_numeric(df["resultObservedValue"], errors="coerce")
df = df.dropna(subset=["resultObservedValue"])

# === Step 4: Filter by parameter and location ===
print(f"Filtering by parameter: {TARGET_PARAMETER}")
print(f"Filtering by location: {TARGET_LOCATION}")
filtered = df[
    (df["observedPropertyDeterminandLabel"] == TARGET_PARAMETER) &
    (df["monitoringSiteIdentifier"] == TARGET_LOCATION)
].copy()

# === Step 5: Group by date and average ===
print("Grouping by date...")
time_series = (
    filtered
    .groupby(DATE_COLUMN)["resultObservedValue"]
    .mean()
    .reset_index()
    .rename(columns={"resultObservedValue": "value"})
)

# === Step 6: Save to file ===
output_path = "timeseries_filtered.csv"
time_series.to_csv(output_path, index=False)
print(f"Done. Time series saved to '{output_path}'")
