from database.general import *

class Predictions (General):

    table_name = "predictions"
    columns = [
        "timeseries_id", 
        "name", 
        "properties", 
        "forecast_property",
        "forecast_correct_points_percent",
        "train_date_from", 
        "train_date_to", 
        "forecast_date_from", 
        "forecast_date_to", 
        "forecast_path"
    ]

    def insert(self, timeseries_id, name, properties, forecast_property, forecast_correct_points_percent, train_date_from, train_date_to, forecast_date_from, forecast_date_to, forecast_path):
        return super().insert({
            "timeseries_id" : timeseries_id,
            "name" : name,
            "properties" : properties,
            "forecast_property": forecast_property,
            "forecast_correct_points_percent": round(forecast_correct_points_percent, 5),
            "train_date_from" : train_date_from,
            "train_date_to" : train_date_to,
            "forecast_date_from" : forecast_date_from,
            "forecast_date_to" : forecast_date_to,
            "forecast_path" : forecast_path,
        })

    def exists(self, name):
        return super().exists({"name" : name})

    def get_id(self, name):
        return super().get_id({"name" : name})

    def delete(self, name):
        return super().delete({"name" : name})