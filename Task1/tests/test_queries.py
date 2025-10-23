import pytest
import psycopg2
from decimal import Decimal
from src.database.query_executor import QueryExecutor
from src.processing.data_importer import DataImporter

def run_query(db_connection, student_path, room_path, query_func):
    """
    Вспомогательная функция, которая очищает БД, загружает данные и выполняет запрос.
    """
    conn = psycopg2.connect(db_connection)
    try:
        query_executor = QueryExecutor(conn)
        query_executor.delete_data()

        importer = DataImporter(conn)
        importer.load_data(student_path=student_path, room_path=room_path)

        result = getattr(query_executor, query_func)()
        return result
    finally:
        conn.close()

@pytest.mark.parametrize("student_path, room_path, expected_result, test_id", [
    (
            'tests/test_data/students.json', 'tests/test_data/rooms.json',
            [('Room A', 2), ('Room B', 1), ('Room C', 1)], "happy_path"
    ),
    (
            'tests/test_data/students_edge_case.json', 'tests/test_data/rooms_edge_case.json',
            [('Room A', 2), ('Room B', 1), ('Empty Room C', 0)], "empty_room"
    ),
    (
            'tests/test_data/students_none.json', 'tests/test_data/rooms.json',
            [('Room A', 0), ('Room B', 0), ('Room C', 0)], "no_students"
    )
])
def test_count_students_into_rooms(db_connection, student_path, room_path, expected_result, test_id):
    result = run_query(db_connection, student_path, room_path, "count_students_into_rooms")
    assert sorted(result) == sorted(expected_result)

@pytest.mark.parametrize("student_path, room_path, expected_result, test_id", [
    (
        'tests/test_data/students.json', 'tests/test_data/rooms.json',
        [('Room A', Decimal('24.0')), ('Room B', Decimal('21.0')), ('Room C', Decimal('20.0'))], "happy_path"
    ),
    (
        'tests/test_data/students_none.json', 'tests/test_data/rooms.json',
        [], "no_students"
    ),
    (
        'tests/test_data/students_edge_case.json', 'tests/test_data/rooms_edge_case.json',
        [('Room A', Decimal('24.0')), ('Room B', Decimal('22.0'))], "fewer_than_5_rooms"
    )
])
def test_rooms_min_avg_age(db_connection, student_path, room_path, expected_result, test_id):
    result = run_query(db_connection, student_path, room_path, "rooms_min_avg_age")
    assert sorted(result) == sorted(expected_result)

@pytest.mark.parametrize("student_path, room_path, expected_result, test_id", [
    (
            'tests/test_data/students.json', 'tests/test_data/rooms.json',
            [('Room A', Decimal('2')), ('Room B', Decimal('0')), ('Room C', Decimal('0'))], "happy_path"
    ),
    (
            'tests/test_data/students_none.json', 'tests/test_data/rooms.json',
            [], "no_students"
    ),
    (
            'tests/test_data/students_same_age.json', 'tests/test_data/rooms.json',
            [('Room A', Decimal('0')), ('Room B', Decimal('0'))], "same_age_in_room"
    )
])
def test_max_age_diff(db_connection, student_path, room_path, expected_result, test_id):
    result = run_query(db_connection, student_path, room_path, "max_age_diff")
    assert sorted(result) == sorted(expected_result)

@pytest.mark.parametrize("student_path, room_path, expected_result, test_id", [
    (
            'tests/test_data/students.json', 'tests/test_data/rooms.json',
            [('Room A',)], "happy_path"
    ),
    (
            'tests/test_data/students_none.json', 'tests/test_data/rooms.json',
            [], "no_students"
    ),
    (
            'tests/test_data/students_no_mix.json', 'tests/test_data/rooms.json',
            [], "no_mixed_rooms"
    )
])
def test_mixed_sex_rooms(db_connection, student_path, room_path, expected_result, test_id):
    result = run_query(db_connection, student_path, room_path, "mixed_sex_rooms")
    assert sorted(result) == sorted(expected_result)