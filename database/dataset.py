from database.general import *

class Dataset (General):

    table_name = "datasets"
    columns = ["name"]

    def insert(self, name):
        return super().insert({"name" : name})

    def is_duplicate(self, name):
        return super().is_duplicate({"name" : name})

    def delete(self, name):
        return super().delete({"name" : name})