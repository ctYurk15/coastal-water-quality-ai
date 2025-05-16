from database.general import *

class Timeseries (General):

    table_name = "timeseries"
    columns = ["name", "dataset_id"]

    def insert(self, name, dataset_id):
        return super().insert({"name" : name, "dataset_id": dataset_id})

    def exists(self, name, dataset_id):
        return super().exists({"name" : name, "dataset_id": dataset_id})

    def get_id(self, name, dataset_id):
        return super().get_id({"name" : name, "dataset_id": dataset_id})

    def delete(self, name, dataset_id):
        return super().delete({"name" : name, "dataset_id": dataset_id})

    def link_location(self, timeseries_id, location_id):
        """
        Вставляє зв'язок між timeseries і location у таблицю timeseries_locations.
        """
        cursor = self.connection.cursor()
        sql = "INSERT INTO timeseries_locations (timeseries_id, location_id) VALUES (%s, %s)"
        values = (timeseries_id, location_id)

        cursor.execute(sql, values)
        self.connection.commit()
        cursor.close()        

    def get_locations(self, timeseries_id):
        """
        Повертає список локацій (id і name), прив’язаних до timeseries_id.
        """
        cursor = self.connection.cursor(dictionary=True)

        sql = """
            SELECT l.id, l.name
            FROM timeseries_locations tl
            JOIN locations l ON tl.location_id = l.id
            WHERE tl.timeseries_id = %s
        """

        cursor.execute(sql, (timeseries_id,))
        rows = cursor.fetchall()
        cursor.close()

        # Повертається список словників: [{'id': 1, 'name': 'Kyiv'}, ...]
        return rows