import mysql.connector
from actions.dataset_parser import *
from database.dataset import *
from database.location import *
from database.timeseries import *
from database.predictions import *
from dotenv import load_dotenv
import os
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
from datetime import datetime
import hashlib
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import matplotlib.dates as mdates

def is_valid_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def generate_filename(length=32):
    random_bytes = os.urandom(16)
    hash_hex = hashlib.sha1(random_bytes).hexdigest()
    return hash_hex[:length]

# load database credentials from .env
load_dotenv()

# form config array
config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

#connect to database
db_connection = mysql.connector.connect(**config)

action = input("You action: ").strip()

match action:
    case "new-dataset":
        dataset_name = input("Dataset name (should be in datasets/ folder): ").strip()
        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)

            if not dataset_processor.exists(dataset_name):
                dataset_id = dataset_processor.insert(dataset_name)
                dataset_data = DatasetParser.getData(dataset_name)

                location_processor = Location(db_connection)

                for loc in dataset_data["locations"]:
                    location_processor.insert(loc, dataset_id)

                print("Dataset with the name `"+dataset_name+"` is successfully added")

            else:
                print("Dataset with the name `"+dataset_name+"` is already processed")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

    case "delete-dataset":
        dataset_name = input("Dataset name (should be in datasets/ folder): ").strip()
        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)
            dataset_processor.delete(dataset_name)

            print("Dataset with the name `"+dataset_name+"` is successfully deleted")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

            
    case "new-timeseries":
        dataset_name = input("Dataset name: ").strip()
        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)
            location_processor = Location(db_connection)
            timeseries_processor = Timeseries(db_connection)

            if dataset_processor.exists(dataset_name):

                dataset_id = dataset_processor.get_id(dataset_name)
                timeseries_name = input("New timeseries name: ").strip()

                if not timeseries_processor.exists(timeseries_name, dataset_id):

                    timeseries_id = timeseries_processor.insert(timeseries_name, dataset_id)
                    
                    location_names = []
                    location_ids = []

                    print("Write locations, for which you want to generate timeseries (write empty value to finish)")

                    while True:
                        name = input("Name: ").strip()
                        if name == "":
                            break

                        if location_processor.exists(name, dataset_id):
                            location_names.append(name)

                            loc_id = location_processor.get_id(name, dataset_id)
                            if loc_id is not None:
                                location_ids.append(loc_id)
                            else:
                                print(f"Internal error: ID for location '{name}' not found despite exists() being True.")
                        else:
                            print(f"Location '{name}' is not found in '{dataset_name}' dataset.")

                    for loc_id in location_ids:
                        timeseries_processor.link_location(timeseries_id, loc_id)

                    print("Timeseries with the name `"+timeseries_name+"` in dataset `"+dataset_name+"` was created for these locations:", location_names)


                else:
                    print("Timeseries with the name `"+timeseries_name+"` already exist in `"+dataset_name+"` dataset")

            else:
                print("Dataset with the name `"+dataset_name+"` is not found in database")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

    case "delete-timeseries":
        dataset_name = input("Dataset name: ").strip()
        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)
            timeseries_processor = Timeseries(db_connection)

            if dataset_processor.exists(dataset_name):

                dataset_id = dataset_processor.get_id(dataset_name)
                timeseries_name = input("Timeseries name: ").strip()

                if timeseries_processor.exists(timeseries_name, dataset_id):
                    timeseries_processor.delete(timeseries_name, dataset_id)

                    print("Timeseries with the name `"+timeseries_name+"` in dataset `"+dataset_name+"` is successfully deleted")
                else:
                    print("Timeseries with the name `"+timeseries_name+"` does not exist for `"+dataset_name+"` dataset")

            else:
                print("Dataset with the name `"+dataset_name+"` is not found in database")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

    case "process-timeseries":
        dataset_name = input("Dataset name: ").strip()

        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)
            timeseries_processor = Timeseries(db_connection)

            if dataset_processor.exists(dataset_name):
                dataset_id = dataset_processor.get_id(dataset_name)
                timeseries_name = input("Timeseries name: ").strip()

                if timeseries_processor.exists(timeseries_name, dataset_id):
                    timeseries_id = timeseries_processor.get_id(timeseries_name, dataset_id)
                    locations = timeseries_processor.get_locations(timeseries_id)

                    print("Ok, generating global timeseries per phenomenon...")

                    # === Load dataset file ===
                    CSV_PATH = os.path.join("datasets", dataset_name)
                    DATE_COLUMN = "phenomenonTimeSamplingDate"

                    df = pd.read_csv(CSV_PATH, low_memory=False)
                    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="%Y%m%d", errors="coerce")
                    df = df.dropna(subset=[DATE_COLUMN])
                    df["resultObservedValue"] = pd.to_numeric(df["resultObservedValue"], errors="coerce")
                    df = df.dropna(subset=["resultObservedValue"])

                    # === Відфільтруємо по дозволених локаціях ===
                    allowed_location_names = [loc["name"] for loc in locations]
                    df = df[df["monitoringSiteIdentifier"].isin(allowed_location_names)]

                    if df.empty:
                        print("No data found for selected locations.")
                        exit()

                    parameters = df["observedPropertyDeterminandLabel"].dropna().unique()

                    OUTPUT_DIR = os.path.join("timeseries", f"{timeseries_id}_{timeseries_name}")
                    os.makedirs(OUTPUT_DIR, exist_ok=True)

                    for param in parameters:
                        param_df = df[df["observedPropertyDeterminandLabel"] == param]

                        ts = (
                            param_df
                            .groupby(DATE_COLUMN)["resultObservedValue"]
                            .mean()
                            .reset_index()
                            .rename(columns={"resultObservedValue": "value"})
                        )

                        safe_name = param.replace('/', '_').replace(' ', '_').replace(':', '_')
                        filename = f"{timeseries_id}_{timeseries_name}_{safe_name}.csv"
                        output_path = os.path.join(OUTPUT_DIR, filename)

                        ts.to_csv(output_path, index=False)
                        print(f"Saved: {output_path} ({len(ts)} rows)")

                    print("All aggregated timeseries saved in:", OUTPUT_DIR)

                else:
                    print(f"Timeseries '{timeseries_name}' not found for dataset '{dataset_name}'")
            else:
                print(f"Dataset '{dataset_name}' not found in database")
        else:
            print(f"No dataset file found in 'datasets/' with the name '{dataset_name}.csv'")
 
    case "predict-timeseries-property":

        """
        TRAIN_START = "2014-01-07"
        TRAIN_END   = "2021-12-31"
        FORECAST_START = "2022-01-01"
        FORECAST_END   = "2022-12-28"
        """

        train_start = input("Training start date: ").strip()
        train_end = input("Training end date: ").strip()
        forecast_start = input("Forecast start date: ").strip()
        forecast_end = input("Forecast end date: ").strip()

        date_inputs = {
            "Training start date": train_start,
            "Training end date": train_end,
            "Forecast start date": forecast_start,
            "Forecast end date": forecast_end
        }

        invalid = [label for label, value in date_inputs.items() if not is_valid_date_format(value)]

        if invalid:
            print("The following dates are in invalid format (expected YYYY-MM-DD):")
            for label in invalid:
                print(f" - {label}: {date_inputs[label]}")
            exit()

        # === User input ===
        dataset_name = input("Dataset name: ").strip()

        if DatasetParser.fileExists(dataset_name):
            dataset_processor = Dataset(db_connection)
            timeseries_processor = Timeseries(db_connection)

            if dataset_processor.exists(dataset_name):
                dataset_id = dataset_processor.get_id(dataset_name)
                timeseries_name = input("Timeseries name: ").strip()

                if timeseries_processor.exists(timeseries_name, dataset_id):

                    timeseries_id = timeseries_processor.get_id(timeseries_name, dataset_id)
                    timeseries_prefix = str(timeseries_id) + "_" + timeseries_name

                    # === Збір і перевірка параметрів ===
                    property_dataframes = {}
                    print("Enter property names one by one (press ENTER without input to finish):")

                    while True:
                        user_input = input("Property name: ").strip()
                        if user_input == "":
                            break

                        cleaned_name = user_input.replace('/', '_').replace(' ', '_').replace(':', '_')
                        path = f'timeseries/{timeseries_prefix}/{timeseries_prefix}_{cleaned_name}.csv'

                        if os.path.isfile(path):
                            try:
                                df = pd.read_csv(path, parse_dates=["phenomenonTimeSamplingDate"])
                                df = df.rename(columns={"phenomenonTimeSamplingDate": "ds", "value": cleaned_name})
                                property_dataframes[cleaned_name] = df[["ds", cleaned_name]]
                                print(f"Added: {cleaned_name}")
                            except Exception as e:
                                print(f"Failed to load {cleaned_name}: {e}")
                        else:
                            print(f"Parameter '{cleaned_name}' not found. Try again.")

                    if not property_dataframes:
                        print("No valid parameters provided.")
                        exit()

                    # === Обираємо цільову змінну ===
                    print("\nAvailable parameters:", ", ".join(property_dataframes.keys()))
                    target = input("Enter the name of the parameter to predict (target): ").strip()

                    if target not in property_dataframes:
                        print(f"'{target}' not in selected parameters.")
                        exit()

                    # === Об'єднуємо всі дані по 'ds' ===
                    from functools import reduce
                    merged_df = reduce(lambda left, right: pd.merge(left, right, on="ds", how="inner"), property_dataframes.values())

                    # === Відкидаємо нульові/від’ємні цільові значення
                    merged_df = merged_df[merged_df[target] > 0]

                    # === Відокремлюємо train/predict
                    train_df = merged_df[(merged_df["ds"] >= train_start) & (merged_df["ds"] <= train_end)]
                    predict_dates = pd.date_range(start=forecast_start, end=forecast_end)

                    if train_df.shape[0] < 5:
                        print("Not enough training data.")
                        exit()

                    # === Створення моделі Prophet
                    model = Prophet(
                        yearly_seasonality=False,
                        weekly_seasonality=False,
                        daily_seasonality=False
                    )
                    for regressor in property_dataframes:
                        if regressor != target:
                            model.add_regressor(regressor)

                    # === Переіменування target → y
                    train_df = train_df.rename(columns={target: "y"})

                    # === Навчання
                    model.fit(train_df)

                    # === Побудова future dataframe
                    future = pd.DataFrame({"ds": predict_dates})
                    for reg in property_dataframes:
                        if reg != target:
                            # беремо з основного об’єднаного датасету регресори для майбутніх дат
                            future = pd.merge(future, merged_df[["ds", reg]], on="ds", how="left")

                    # === Очистка future: видалення рядків з NaN у регресорах
                    required_columns = [col for col in future.columns if col != "ds"]
                    future = future.dropna(subset=required_columns)

                    # === Прогноз
                    forecast = model.predict(future)

                    # === Автоматичне обрізання піків у прогнозі ===
                    yhat_values = forecast["yhat"]
                    median = yhat_values.median()
                    std = yhat_values.std()

                    # Верхня межа: медіана + 3 стандартних відхилення
                    #clip_upper = median + std
                    # === Обрізання по 95-му процентилю
                    #clip_upper = np.percentile(forecast["yhat"], 95)
                    #forecast["yhat"] = forecast["yhat"].clip(upper=clip_upper)

                    # === Окреме завантаження тільки target для actual_df
                    #actual_df = merged_df[(merged_df["ds"] >= forecast_start) & (merged_df["ds"] <= forecast_end)]
                    target_path = f'timeseries/{timeseries_prefix}/{timeseries_prefix}_{target}.csv'
                    actual_df = pd.read_csv(target_path, parse_dates=["phenomenonTimeSamplingDate"])
                    actual_df = actual_df.rename(columns={"phenomenonTimeSamplingDate": "ds", "value": target})
                    actual_df = actual_df[["ds", target]]
                    actual_df = actual_df[(actual_df["ds"] >= forecast_start) & (actual_df["ds"] <= forecast_end)]

                    # === Обмеження на основі реальних значень
                    historical_max = actual_df[target].max()
                    historical_min = actual_df[target].min()

                    clip_upper = historical_max
                    clip_lower = historical_min

                    # Обмеження прогнозу зверху і знизу
                    forecast["yhat"] = forecast["yhat"].clip(lower=clip_lower, upper=clip_upper)

                    # === Об'єднуємо для аналізу
                    merged_eval = pd.merge(
                        actual_df[["ds", target]],
                        forecast[["ds", "yhat"]],
                        on="ds",
                        how="inner"
                    )

                    # === Обчислюємо помилки
                    merged_eval["abs_error"] = abs(merged_eval[target] - merged_eval["yhat"])
                    merged_eval["pct_error"] = merged_eval["abs_error"] / (merged_eval[target].replace(0, np.nan)) * 100  # уникаємо ділення на 0

                    # === Порог точності (%)
                    threshold_pct = 20  # Точка вважається "хорошою", якщо похибка ≤ 20%
                    merged_eval["good"] = merged_eval["pct_error"] <= threshold_pct
                    good_ratio = merged_eval["good"].sum() / merged_eval.shape[0] * 100
                    good_text = f"Good predictions: {good_ratio:.1f}% ≤ {threshold_pct}% error"

                    # === Збереження в БД
                    folder_name = generate_filename()
                    predictions_processor = Predictions(db_connection)
                    prediction_id = predictions_processor.insert(
                        timeseries_id,
                        input("Prediction name: "),
                        ",".join(property_dataframes.keys()),
                        target, 
                        good_ratio,
                        train_start,
                        train_end,
                        forecast_start,
                        forecast_end,
                        folder_name
                    )
                    folder_name = str(prediction_id) + "_" + folder_name

                    # === Побудова графіків
                    output_dir = os.path.join("forecasts", timeseries_prefix, folder_name)
                    os.makedirs(output_dir, exist_ok=True)

                    # 1. Реальні значення
                    plt.figure(figsize=(20, 10))
                    plt.plot(actual_df["ds"], actual_df[target], color="red", label="Actual")
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    plt.xticks(rotation=45)
                    plt.title(f"Actual Data ({target})")
                    plt.xlabel("Date")
                    plt.ylabel("Value")
                    plt.legend()
                    plt.grid(True)
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_dir, "actual.png"))
                    plt.close()

                    # 2. Прогнозовані значення
                    plt.figure(figsize=(20, 10))
                    plt.plot(forecast["ds"], forecast["yhat"], color="blue", label="Predicted")
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    plt.xticks(rotation=45)
                    plt.title(f"Predicted Data ({target})")
                    plt.xlabel("Date")
                    plt.ylabel("Value")
                    plt.legend()
                    plt.grid(True)
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_dir, "predicted.png"))
                    plt.close()

                    # === 3. Comparison between preiction and reality

                    """
                    # === Combined графік з точками "good"
                    plt.figure(figsize=(10, 6))
                    plt.plot(merged_eval["ds"], merged_eval["yhat"], label="Predicted", color="blue")
                    plt.plot(merged_eval["ds"], merged_eval[target], label="Actual", color="red")

                    # Додаємо "хороші" точки
                    plt.scatter(good_points["ds"], good_points["yhat"], color="green", s=20, label="Good points")
                    """
                    good_points = merged_eval[merged_eval["good"]]
                    
                    plt.figure(figsize=(20, 10))
                    plt.plot(forecast["ds"], forecast["yhat"], label="Predicted", color="blue")
                    plt.plot(actual_df["ds"], actual_df[target], label="Actual", color="red")
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    plt.xticks(rotation=45)
                    plt.scatter(good_points["ds"], good_points["yhat"], color="green", s=20, label="Good points")
                    plt.title(f"Forecast vs Real ({target})\n{good_text}\nLimit: {clip_upper:.2f}")
                    plt.xlabel("Date")
                    plt.ylabel("Value")
                    plt.legend()
                    plt.grid(True)
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_dir, "combined.png"))
                    plt.close()

                    # === 4. Error-графік: абсолютна похибка
                    plt.figure(figsize=(20, 10))
                    plt.plot(merged_eval["ds"], merged_eval["abs_error"], color="purple", label="|y - ŷ|")
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
                    plt.xticks(rotation=45)
                    plt.title(f"Absolute Error per Date ({target})")
                    plt.xlabel("Date")
                    plt.ylabel("Absolute Error")
                    plt.tight_layout()
                    plt.legend()
                    plt.grid(True)
                    plt.savefig(os.path.join(output_dir, "error.png"))
                    plt.close()

                    print(f"Forecast for '{target}' saved in {output_dir}. Result: {good_ratio}%")
                else:
                    print(f"Timeseries '{timeseries_name}' not found for dataset '{dataset_name}'")
            else:
                print(f"Dataset '{dataset_name}' not found in database")
        else:
            print(f"No dataset file found in 'datasets/' with the name '{dataset_name}.csv'")

    case _:
        print("What?")