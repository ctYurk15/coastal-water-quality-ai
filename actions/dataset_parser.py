import pandas

class DatasetParser:

    datasets_path = "datasets/"

    @staticmethod
    def getData(file_name):
        df = pandas.read_csv(DatasetParser.datasets_path+"/"+file_name, usecols=["monitoringSiteIdentifier"], low_memory=False)

        counts = (
            df["monitoringSiteIdentifier"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "monitoringSiteIdentifier", "monitoringSiteIdentifier": "measurement_count"})
        )

        return counts