import mysql.connector
from actions.dataset_parser import *
from database.dataset import *
from database.location import *
from database.timeseries import *
from dotenv import load_dotenv
import os
import pandas as pd

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
            location_processor = Location(db_connection)

            if not dataset_processor.exists(dataset_name):
                dataset_id = dataset_processor.insert(dataset_name)
                dataset_data = DatasetParser.getData(dataset_name)

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

    case _:
        print("What?")