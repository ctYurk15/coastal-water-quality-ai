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

                    property_name = input("Property name: ").strip()
                    property_name = property_name.replace('/', '_').replace(' ', '_').replace(':', '_')
                    timeseries_prefix = str(timeseries_id) + "_" + timeseries_name

                    # === Path to CSV ===
                    property_timeseries_path = f'timeseries/{timeseries_prefix}/{timeseries_prefix}_{property_name}.csv'

                    if os.path.isfile(property_timeseries_path):

                        prediction_name = input("Prediction name: ")

                        print('File found, running forecast...')

                        # === Load and prepare ===
                        try:
                            df = pd.read_csv(property_timeseries_path, parse_dates=["phenomenonTimeSamplingDate"])
                        except Exception as e:
                            print(f"Failed to read file: {e}")
                            exit()

                        df = df.rename(columns={
                            "phenomenonTimeSamplingDate": "ds",
                            "value": "y"
                        })

                        df = df[df["y"] > 0]
                        train_df = df[(df["ds"] >= train_start) & (df["ds"] <= train_end)]

                        if train_df.shape[0] < 5:
                            print("Not enough training data.")
                            exit()

                        # === Train model ===
                        model = Prophet()
                        model.fit(train_df)

                        # === Forecast future ===
                        forecast_dates = pd.date_range(start=forecast_start, end=forecast_end)
                        future = pd.concat([
                            train_df[["ds"]],
                            pd.DataFrame({"ds": forecast_dates})
                        ]).drop_duplicates().reset_index(drop=True)

                        forecast = model.predict(future)
                        forecast_range = forecast[
                            (forecast["ds"] >= forecast_start) &
                            (forecast["ds"] <= forecast_end)
                        ]

                        # === Фактичні дані в періоді прогнозу ===
                        # === Фільтруємо прогноз і фактичні значення лише для періоду передбачення ===
                        actual_forecast_df = df[(df["ds"] >= forecast_start) & (df["ds"] <= forecast_end)]
                        predicted_df = forecast[(forecast["ds"] >= forecast_start) & (forecast["ds"] <= forecast_end)]

                        # === Перевірка на наявність даних ===
                        if actual_forecast_df.empty or predicted_df.empty:
                            print("Not enough data in forecast period to plot comparison.")
                            exit()

                        # === Папка для збереження ===
                        filename = generate_filename()
                        forecast_output_dir = os.path.join("forecasts", timeseries_prefix, filename)
                        os.makedirs(forecast_output_dir, exist_ok=True)

                        # === 1. Графік тільки реальних значень ===
                        plt.figure(figsize=(10, 6))
                        plt.plot(actual_forecast_df["ds"], actual_forecast_df["y"], color="red", label="Actual")
                        plt.title(f"Actual Data ({property_name})\n{forecast_start} to {forecast_end}")
                        plt.xlabel("Date")
                        plt.ylabel("Value")
                        plt.legend()
                        plt.tight_layout()
                        actual_path = os.path.join(forecast_output_dir, "actual.png")
                        plt.savefig(actual_path)
                        plt.close()

                        # === 2. Графік тільки прогнозу ===
                        plt.figure(figsize=(10, 6))
                        plt.plot(predicted_df["ds"], predicted_df["yhat"], color="blue", label="Predicted")
                        plt.title(f"Predicted Data ({property_name})\n{forecast_start} to {forecast_end}")
                        plt.xlabel("Date")
                        plt.ylabel("Value")
                        plt.legend()
                        plt.tight_layout()
                        predicted_path = os.path.join(forecast_output_dir, "predicted.png")
                        plt.savefig(predicted_path)
                        plt.close()

                        # === 3. Об'єднаний графік ===
                        plt.figure(figsize=(10, 6))
                        plt.plot(predicted_df["ds"], predicted_df["yhat"], label="Predicted", color="blue")
                        plt.plot(actual_forecast_df["ds"], actual_forecast_df["y"], label="Actual", color="red")
                        plt.title(f"Forecast vs Real ({property_name})\n{forecast_start} to {forecast_end}")
                        plt.xlabel("Date")
                        plt.ylabel("Value")
                        plt.legend()
                        plt.tight_layout()
                        combined_path = os.path.join(forecast_output_dir, "combined.png")
                        fig = plt.gcf()
                        plt.savefig(combined_path)
                        plt.close()

                        # === Збереження запису в БД ===
                        predictions_processor = Predictions(db_connection)
                        predictions_processor.insert(
                            timeseries_id,
                            prediction_name,
                            property_name,
                            train_start,
                            train_end,
                            forecast_start,
                            forecast_end,
                            filename
                        )

                        print(f"Forecasts saved to: {forecast_output_dir}")

                    else:
                        print(f"Parameter '{property_name}' is not found in timeseries '{timeseries_name}' of dataset '{dataset_name}'")
                else:
                    print(f"Timeseries '{timeseries_name}' not found for dataset '{dataset_name}'")
            else:
                print(f"Dataset '{dataset_name}' not found in database")
        else:
            print(f"No dataset file found in 'datasets/' with the name '{dataset_name}.csv'")

    case _:
        print("What?")