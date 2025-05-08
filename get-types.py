import pandas as pd

# Шлях до великого CSV-файлу
csv_path = "datasets/Waterbase_v2023_1_T_WISE6_DisaggregatedData.csv"

# Назва колонки, з якої витягуємо унікальні значення
column_name = "parameterWaterBodyCategory"

# Створюємо порожній набір для збору унікальних значень
unique_values = set()

# Читання CSV по частинах (наприклад, по 100 000 рядків)
chunksize = 100_000

# Обробка великих файлів по частинах
for chunk in pd.read_csv(csv_path, usecols=[column_name], chunksize=chunksize):
    # Додаємо унікальні значення з кожного блоку
    unique_values.update(chunk[column_name].dropna().unique())

# Вивід унікальних значень
print(f"Found {len(unique_values)} unique values in column '{column_name}':")
for value in sorted(unique_values):
    print(value)
