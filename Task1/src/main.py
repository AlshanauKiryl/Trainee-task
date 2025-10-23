import sys
import logging
from .database.database_connector import DatabaseConnection
from .database.query_executor import QueryExecutor
from .processing.data_importer import DataImporter
from .processing.data_exporter import DataExporter

def main():
    """Основная функция приложения."""
    logging.basicConfig(level=logging.INFO, filename="./logs/py_log.log", filemode="w")

    if len(sys.argv) > 3:
        students_path = sys.argv[1]
        rooms_path = sys.argv[2]
        output_format = sys.argv[3]
    else:
        students_path = './data/students.json'
        rooms_path = './data/rooms.json'
        output_format = 'json'

    exporter = DataExporter()

    with DatabaseConnection() as conn:
        query_executor = QueryExecutor(conn)

        query_executor.delete_data()

        importer = DataImporter(conn)
        importer.load_data(students_path, rooms_path)

        query_executor.create_indexes()

        results = {
            "output1": query_executor.count_students_into_rooms,
            "output2": query_executor.rooms_min_avg_age,
            "output3": query_executor.max_age_diff,
            "output4": query_executor.mixed_sex_rooms
        }

        for filename, query_func in results.items():
            result = query_func()
            exporter.dump_data(result, output_format, filename)


if __name__ == "__main__":
    main()