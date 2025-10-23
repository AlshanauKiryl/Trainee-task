import pytest
from python_on_whales import DockerClient
import psycopg2
import time
import os
import sys
import shutil  # <-- ДОБАВИТЬ ЭТОТ ИМПОРТ


@pytest.fixture(scope="session")
def db_connection():
    """
    Фикстура, которая делает всё волшебство:
    1. Запускает Docker Compose перед всеми тестами.
    2. Ждет, пока база данных будет готова принимать подключения.
    3. Предоставляет (yield) URL для подключения к БД.
    4. Останавливает и очищает всё после завершения всех тестов.
    """
    project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    compose_file = os.path.join(project_dir, 'docker-compose.yml')
    client_kwargs = {"compose_files": [compose_file]}

    if sys.platform == "win32":
        docker_exe_path = shutil.which("docker.exe")
        if docker_exe_path:
            client_kwargs["client_call"] = [docker_exe_path]

    docker = DockerClient(**client_kwargs)

    try:
        print("\n[+] Запускаем тестовую базу данных...")
        docker.compose.up(
            detach=True,
            build=True,
            force_recreate=True
        )

        retries = 10
        delay = 3
        conn_url = "postgresql://postgres:postgres@localhost:5432/task1_db"

        for i in range(retries):
            try:
                conn = psycopg2.connect(conn_url)
                conn.close()
                print(">>> База данных готова!")
                break
            except psycopg2.OperationalError:
                print(f"--- Ожидание базы данных... (попытка {i + 1}/{retries})")
                time.sleep(delay)
        else:
            pytest.fail("Не удалось подключиться к тестовой базе данных.")

        yield conn_url

    finally:
        print("\n[-] Останавливаем тестовую базу данных...")
        docker.compose.down(volumes=True)