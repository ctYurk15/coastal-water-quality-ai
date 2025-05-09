import pandas as pd

# Шлях до великого CSV-файлу
csv_path = "datasets/Waterbase_v2023_1_T_WISE6_DisaggregatedData.csv"

# Зчитуємо лише заголовок (перший рядок)
df = pd.read_csv(csv_path, nrows=0)

# Виводимо список колонок
print("List of columns in the file:")
for col in df.columns:
    print(col)