import pandas as pd
import matplotlib.pyplot as plt
import os
from prophet import Prophet

# === Input ===
location_id = input("Enter monitoring site ID (default = EESJA0094000): ").strip()
if not location_id:
    location_id = "EESJA0094000"

parameter_file = input("Enter parameter filename (e.g., Secchi_depth.csv): ").strip()
if not parameter_file.endswith(".csv"):
    print("Invalid filename. It must end with .csv")
    exit()

# === Paths ===
input_path = os.path.join("timeseries", location_id, parameter_file)
output_dir = os.path.join("forecasts", location_id)
os.makedirs(output_dir, exist_ok=True)

# === Load time series ===
try:
    df = pd.read_csv(input_path, parse_dates=["phenomenonTimeSamplingDate"])
except FileNotFoundError:
    print(f"File not found: {input_path}")
    exit()

# === Prepare for Prophet ===
df = df.rename(columns={
    "phenomenonTimeSamplingDate": "ds",
    "value": "y"
})

# Remove negative/zero values if needed
df = df[df["y"] > 0]

if df.shape[0] < 5:
    print("Not enough data to train forecast model.")
    exit()

# === Train Prophet ===
model = Prophet()
model.fit(df)

# === Create future dataframe ===
future = model.make_future_dataframe(periods=365)
forecast = model.predict(future)

# === Plot forecast ===
fig = model.plot(forecast)
plt.title(f"Forecast for {parameter_file.replace('.csv', '')}")
plt.xlabel("Date")
plt.ylabel("Predicted value")
plt.tight_layout()

# === Save forecast chart ===
output_path = os.path.join(output_dir, f"{parameter_file.replace('.csv', '')}_forecast.png")
fig.savefig(output_path)
plt.close()

print(f"Forecast chart saved to: {output_path}")
