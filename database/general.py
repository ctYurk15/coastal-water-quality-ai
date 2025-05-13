class General:

    table_name = ""
    columns = []

    def __init__(self, connection):
        self.connection = connection

    def insert(self, values: dict):
        cursor = self.connection.cursor()

        # Перевірка, чи всі ключі з values є в columns
        if not set(values.keys()).issubset(set(self.columns)):
            raise ValueError("Invalid columns in values dictionary")

        # Формування SQL-запиту
        cols = ", ".join(values.keys())
        placeholders = ", ".join(["%s"] * len(values))
        sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"

        # Отримання значень у правильному порядку
        val_tuple = tuple(values[col] for col in values)

        # Виконання запиту
        cursor.execute(sql, val_tuple)
        self.connection.commit()
        last_id = cursor.lastrowid
        cursor.close()
        return last_id

    def is_duplicate(self, values: dict) -> bool:
        """
        Перевіряє, чи існує запис з такими ж значеннями, як у переданих полях.
        """
        cursor = self.connection.cursor()

        # Формуємо WHERE-умову на основі переданих ключів
        conditions = " AND ".join([f"{col} = %s" for col in values])
        sql = f"SELECT COUNT(*) FROM {self.table_name} WHERE {conditions}"
        val_tuple = tuple(values[col] for col in values)

        cursor.execute(sql, val_tuple)
        count = cursor.fetchone()[0]
        cursor.close()

        return count > 0

    def delete(self, conditions: dict) -> int:
        """
        Видаляє записи з таблиці за вказаними умовами.
        Повертає кількість видалених рядків.
        """
        if not conditions:
            raise ValueError("Deletion requires at least one condition")

        if not set(conditions.keys()).issubset(set(self.columns)):
            raise ValueError("Invalid columns in deletion conditions")

        cursor = self.connection.cursor()
        where_clause = " AND ".join([f"{col} = %s" for col in conditions])
        sql = f"DELETE FROM {self.table_name} WHERE {where_clause}"
        val_tuple = tuple(conditions[col] for col in conditions)

        cursor.execute(sql, val_tuple)
        self.connection.commit()
        deleted = cursor.rowcount
        cursor.close()
        return deleted