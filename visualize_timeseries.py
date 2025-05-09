import pandas as pd
import matplotlib.pyplot as plt
import os

# === User input ===
location_id = input("Enter monitoring site ID (default = EESJA0094000): ").strip()
if not location_id:
    location_id = "EESJA0094000"

parameter_file = input("Enter parameter filename (e.g., Secchi_depth.csv) or leave empty for ALL: ").strip()

# === Paths ===
input_dir = os.path.join("timeseries", location_id)
output_dir = os.path.join("charts", location_id)
os.makedirs(output_dir, exist_ok=True)

# === Get list of files ===
if parameter_file:
    filenames = [parameter_file]
else:
    try:
        filenames = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
        if not filenames:
            print(f"No CSV files found in: {input_dir}")
            exit()
    except FileNotFoundError:
        print(f"Directory not found: {input_dir}")
        exit()

# === Plot each file ===
for file in filenames:
    input_path = os.path.join(input_dir, file)
    try:
        df = pd.read_csv(input_path, parse_dates=["phenomenonTimeSamplingDate"])
    except Exception as e:
        print(f"Failed to read {file}: {e}")
        continue

    # Plot
    plt.figure(figsize=(10, 5))
    plt.plot(df["phenomenonTimeSamplingDate"], df["value"], marker='o', linestyle='-')
    plt.title(f"{file.replace('.csv', '')} at {location_id}")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.grid(True)
    plt.tight_layout()

    # Save plot
    output_path = os.path.join(output_dir, f"{file.replace('.csv', '')}.png")
    plt.savefig(output_path)
    plt.close()
    print(f"Saved chart: {output_path}")

print("All charts generated.")
