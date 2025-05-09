import pandas as pd

# Input and output file paths
input_csv = "datasets/Waterbase_v2023_1_T_WISE6_DisaggregatedData.csv"
output_csv = "datasets/coastal_water_only.csv"

# Column to filter on
target_column = "parameterWaterBodyCategory"
target_value = "CW"

# Columns can be loaded dynamically, or you can specify them if known
print("Starting filtering process...")
chunksize = 100_000
chunk_num = 0
rows_written = 0

# Open output file and write header
with pd.read_csv(input_csv, chunksize=chunksize, low_memory=False) as reader:
    for i, chunk in enumerate(reader):
        # Filter rows where water type is "CW"
        filtered_chunk = chunk[chunk[target_column] == target_value]

        if not filtered_chunk.empty:
            if i == 0 or rows_written == 0:
                # First write with header
                filtered_chunk.to_csv(output_csv, mode='w', index=False)
            else:
                # Append without header
                filtered_chunk.to_csv(output_csv, mode='a', index=False, header=False)
            rows_written += len(filtered_chunk)

        chunk_num += 1
        if chunk_num % 10 == 0:
            print(f"Processed {chunk_num * chunksize:,} rows...")

print(f"Done! Total rows written: {rows_written}")
print(f"Filtered data saved to '{output_csv}'")
