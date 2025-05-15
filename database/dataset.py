from database.general import *

class Dataset (General):

    table_name = "datasets"
    columns = ["name"]

    def insert(self, name):
        return super().insert({"name" : name})

    def exists(self, name):
        return super().exists({"name" : name})

    def get_id(self, name):
        return super().get_id({"name" : name})

    def delete(self, name):
        return super().delete({"name" : name})