import logging

class QueryExecutor:
    """Класс для выполнения SQL-запросов к базе данных."""
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def delete_data(self):
        """Удаляет все записи из таблиц students и rooms."""
        self.cursor.execute("DELETE FROM students")
        self.cursor.execute("DELETE FROM rooms")
        logging.info('Deleting records from students and rooms')
        self.conn.commit()

    def create_indexes(self):
        """Создает индексы для таблиц."""
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_room_id ON students(room_id);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_birthday ON students(birthday);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_sex ON students(sex);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_room_name ON rooms(name);")
        logging.info('Creating indexes')
        self.conn.commit()

    def count_students_into_rooms(self):
        """Подсчитывает количество студентов в каждой комнате."""
        self.cursor.execute("""
            SELECT rooms.name, COUNT(students.id) AS student_count
            FROM rooms
            LEFT JOIN students ON rooms.id = students.room_id
            GROUP BY rooms.id, rooms.name
            ORDER BY rooms.name;
        """)
        return self.cursor.fetchall()

    def rooms_min_avg_age(self):
        """Находит 5 комнат с минимальным средним возрастом студентов."""
        self.cursor.execute("""
            SELECT rooms.name, AVG(EXTRACT(YEAR FROM AGE(NOW(), students.birthday))) AS avg_age
            FROM rooms
            JOIN students ON rooms.id = students.room_id
            GROUP BY rooms.id, rooms.name
            ORDER BY avg_age
            LIMIT 5;
        """)
        return self.cursor.fetchall()

    def max_age_diff(self):
        """Находит 5 комнат с максимальной разницей в возрасте студентов."""
        self.cursor.execute("""
            SELECT rooms.name, MAX(EXTRACT(YEAR FROM AGE(NOW(), students.birthday))) -
                   MIN(EXTRACT(YEAR FROM AGE(NOW(), students.birthday))) AS age_diff
            FROM rooms
            JOIN students ON rooms.id = students.room_id
            GROUP BY rooms.id, rooms.name
            ORDER BY age_diff DESC
            LIMIT 5;
        """)
        return self.cursor.fetchall()

    def mixed_sex_rooms(self):
        """Находит комнаты, в которых живут студенты разного пола."""
        self.cursor.execute("""
            SELECT rooms.name
            FROM students
            INNER JOIN rooms ON students.room_id = rooms.id
            GROUP BY rooms.id
            HAVING COUNT(DISTINCT students.sex) > 1
            ORDER BY rooms.id;
        """)
        return self.cursor.fetchall()