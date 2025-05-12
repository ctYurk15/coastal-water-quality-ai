import mysql.connector
from actions.dataset_parser import *
from database.dataset import *
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
            db_processor = Dataset(db_connection)

            if not db_processor.is_duplicate(dataset_name):
                dataset_id = db_processor.insert(dataset_name)
                unique_locations = DatasetParser.getData(dataset_name)

                print(unique_locations.to_string(index=False))

            else:
                print("Dataset with the name `"+dataset_name+"` is already processed")
        else:
            print("No dataset found in folder `datasets` with the name `"+dataset_name+"`")

    case _:
        print("What?")