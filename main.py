import mysql.connector
from actions.dataset_parser import *

action = input("You action: ").strip()

match action:
    case "new-dataset":
        dataset_name = input("Dataset name (should be in datasets/ folder): ").strip()
        unique_locations = DatasetParser.getData(dataset_name)

        print(unique_locations.to_string(index=False))

    case _:
        print("What?")