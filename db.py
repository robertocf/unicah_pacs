import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    conn = psycopg2.connect(
        dbname="pacsdb",
        user="postgres",
        password="roberto",
        host="10.2.0.10",
        port="5432",
    )
    return conn
