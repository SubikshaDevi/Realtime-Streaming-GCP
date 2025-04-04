import os
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text
import pandas as pd

# Load environment variables from .env file
load_dotenv()

def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of PostgreSQL.
    """
    # Fetch database credentials from environment variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")  # 'project:region:instance'
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_name = os.getenv("DB_NAME")
    private_ip = os.getenv("PRIVATE_IP", "false").lower() == "true"

    if not all([instance_connection_name, db_user, db_pass, db_name]):
        raise ValueError("Missing required database environment variables.")

    # Determine IP type (private or public)
    ip_type = IPTypes.PRIVATE if private_ip else IPTypes.PUBLIC

    # Initialize Cloud SQL Connector
    connector = Connector(refresh_strategy="LAZY")

    def getconn() -> pg8000.dbapi.Connection:
        return connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )

    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )

    return engine

# Example usage
def test_connection():
    engine = connect_with_connector()
    # with engine.connect() as connection:
    #     result = connection.execute(text("SELECT * from votes"))
    #     for row in result:
    #         print("Connected successfully, current time:", row[0])


    with engine.connect() as connection:
        df = pd.read_sql(text("SELECT * FROM votes"), connection)
        
    print(df)
if __name__ == "__main__":
    test_connection()