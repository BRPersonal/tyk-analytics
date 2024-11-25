import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

def write_to_table(table_name:str, data_frame:pd.core.frame.DataFrame) -> None:
    try:
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor = connection.cursor()
            delete_query = f"DELETE FROM {table_name};"
            cursor.execute(delete_query)
            connection.commit()
            print(f"Deleted existing data from {table_name}.")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")
