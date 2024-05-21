import fastapi
from .sensors.controller import router as sensorsRouter
from psycopg2 import connect, OperationalError
import time
import psycopg2
from yoyo import get_backend, read_migrations, step

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")
from app.cassandra_client import CassandraClient
def get_database_connection_string():
    retries = 5
    while retries:
        try:
            return 'postgres://timescale:timescale@timescale:5433/timescale'
        except OperationalError as e:
            retries -= 1
            print("Database connection failed. Retrying...", str(e))
            time.sleep(10)  # wait 10 seconds before trying to reconnect
    raise Exception("Failed to connect to database after several attempts.")

def get_real_database_connection():
    retries = 5
    dsn = 'postgres://timescale:timescale@timescale:5433/timescale'
    while retries:
        try:
            conn = psycopg2.connect(dsn=dsn)
            conn.autocommit = True  # Set autocommit to True to execute commands outside of transactions
            return conn
        except psycopg2.OperationalError as e:
            retries -= 1
            print("Real database connection failed. Retrying...", str(e))
            time.sleep(10)
    raise Exception("Failed to connect to database after several attempts.")

def execute_view_creation():
    conn = get_real_database_connection()
    try:
        with conn.cursor() as cursor:
            with open('./views/views_migrations.sql', 'r') as file:
                sql_commands = file.read().split(';')  # Dividir en comandos individuales
                for command in sql_commands:
                    if command.strip():
                        cursor.execute(command)
    finally:
        conn.close()
        

def apply_migrations():
    connection_string = get_database_connection_string()
    backend = get_backend(connection_string)
    with backend.lock():
        try:
            with get_real_database_connection() as conn:
                with conn.cursor() as cursor:
                    #to recreate the tables
                    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '_yoyo_migration');")
                    exists_migration = cursor.fetchone()[0]
                    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '_yoyo_log');")
                    exists_log = cursor.fetchone()[0]
                    if exists_migration:
                        cursor.execute("DELETE FROM _yoyo_migration;")
                    if exists_log:
                        cursor.execute("DELETE FROM _yoyo_log;")
                    conn.commit()

        except Exception as e:
            print(f"Error verifying or deleting records from Yoyo tables: {e}")
            return 
        migrations = read_migrations('./migrations_ts')
        backend.apply_migrations(backend.to_apply(migrations))
        # backend.rollback()
    try:
        execute_view_creation()
    except Exception as e:
        print(f"Error checking views: {e}")

def create_cassandra():
    cassandra_client = CassandraClient(["cassandra"])
    session = cassandra_client.get_session()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {
            'class': 'SimpleStrategy', 
            'replication_factor': '1'
        } AND durable_writes = true;
    """)
    session.set_keyspace('sensor')
    session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_temperatures (
            sensor_id INT,
            temperature double,
            last_seen TIMESTAMP,
            PRIMARY KEY (sensor_id, last_seen )) WITH CLUSTERING ORDER BY (last_seen DESC);
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_counts (
        sensor_type text,
        sensor_id INT,
        PRIMARY KEY (sensor_type, sensor_id)
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS sensors_low_battery (
            battery_range text,
            battery_level double,
            sensor_id INT,
            PRIMARY KEY (battery_range, sensor_id)
        ) WITH CLUSTERING ORDER BY (sensor_id ASC);
    """)
    
    cassandra_client.close()

        
app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
