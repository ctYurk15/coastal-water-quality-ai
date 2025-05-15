from database.general import *

class Location (General):

    table_name = "locations"
    columns = ["name", "dataset_id"]

    def insert(self, name, dataset_id):
        return super().insert({"name" : name, "dataset_id": dataset_id})

    def exists(self, name, dataset_id):
        return super().exists({"name" : name, "dataset_id": dataset_id})

    def get_id(self, name, dataset_id):
        return super().get_id({"name" : name, "dataset_id": dataset_id})

    def delete(self, name, dataset_id):
        return super().delete({"name" : name, "dataset_id": dataset_id})