import mysql.connector
from actions.dataset_parser import *
from database.dataset import *
from database.location import *
from dotenv import load_dotenv
import os

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

            if not dataset_processor.existsдя(dataset_name):
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

            if dataset_processor.exists(dataset_name):

                dataset_id = dataset_processor.get_id(dataset_name)

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

                print("\nTotal locations found:", len(location_names))
                print("Names:", location_names)
                print("IDs:", location_ids)

            else:
                print("Dataset with the name `"+dataset_name+"` is not found in database")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

    case _:
        print("What?")