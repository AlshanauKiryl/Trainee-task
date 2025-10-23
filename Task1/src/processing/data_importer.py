import json
import logging

class DataImporter:
    """Класс для импорта данных в базу данных."""
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def load_data(self, student_path, room_path):
        """Загружает данные из json файлов в базу данных."""
        self._load_rooms(room_path)
        self._load_students(student_path)
        logging.info(f'Importing data from {student_path} and {room_path}')
        self.conn.commit()

    def _load_rooms(self, room_path):
        with open(room_path, 'r', encoding='utf-8') as f:
            for room in json.load(f):
                self.cursor.execute('INSERT INTO rooms (id, name) VALUES (%s, %s)', (room["id"], room["name"]))

    def _load_students(self, student_path):
        with open(student_path, 'r', encoding='utf-8') as f:
            for student in json.load(f):
                self.cursor.execute(
                    'INSERT INTO students (birthday, id, name, room_id, sex) VALUES (%s, %s, %s, %s, %s)', (
                        student["birthday"],
                        student["id"],
                        student["name"],
                        student["room"],
                        student["sex"],
                    )
                )