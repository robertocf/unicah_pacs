import psycopg2
from psycopg2.extras import RealDictCursor
from config import SERVER_IP, SERVER_PASS, SERVER_USER, SERVER_DB, SERVER_PORT

def get_db_connection():
    conn = psycopg2.connect(
        dbname=f'{SERVER_DB}',
        user=f'{SERVER_USER}',
        password=f'{SERVER_PASS}',
        host=f'{SERVER_IP}',
        port=f'{SERVER_PORT}',
    )
    return conn
