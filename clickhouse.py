from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")  
POSTGRES_HOST= os.getenv("POSTGRES_HOST")
DB_USER = os.getenv("DB_USER")
DB_PWD = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")

    
engine = create_engine(
    f"clickhouse+http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
)
print(engine)


def execute_query(query):
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
        print(df)
    return df
   
   
def create_tables():
    table_name = ['votes','rides','payments','driver','pricing','cancellations','driver_availablity','locations']
    #users
    for i in table_name: 
        create_table_query = f"""
                CREATE TABLE {i}_pg
            ENGINE = PostgreSQL('{POSTGRES_HOST}:{DB_PORT}', '{DB_NAME}', '{i}', '{DB_USER}', '{DB_PWD}');
        """
        execute_query(create_table_query)


def insert_clickhouse():
    table_name = ['rides','payments','driver','pricing','cancellations','driver_availablity','locations']
    
    for i in table_name:
        insert_query = f"""
            INSERT INTO {i} SELECT * FROM {i}_pg;"""
            
        execute_query(insert_query)

if __name__ ==  "__main__":
    # create_tables()
    # insert_clickhouse()
    execute_query('select * from users_pg')
    



