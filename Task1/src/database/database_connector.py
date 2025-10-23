import os
import psycopg2 as postgre
from dotenv import load_dotenv
import logging

load_dotenv()

class DatabaseConnection:
    """Класс для управления подключением к базе данных."""
    def __init__(self):
        self.conn = None

    def __enter__(self):
        try:
            self.conn = postgre.connect(os.getenv('DB_URL'))
            logging.info('Connection to DB established')
            return self.conn
        except postgre.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            logging.info('Connection to DB closed')