import pandas
import os

class DatasetParser:

    datasets_path = "datasets/"

    @staticmethod
    def fileExists(file_name):
        return os.path.isfile(DatasetParser.datasets_path+"/"+file_name)

    @staticmethod
    def getData(file_name):
        df = pandas.read_csv(DatasetParser.datasets_path+"/"+file_name, usecols=["monitoringSiteIdentifier"], low_memory=False)

        locations = df["monitoringSiteIdentifier"].drop_duplicates().reset_index(drop=True)

        return {"locations": locations}