import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
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

def write_analytics_data(data_frame:pd.core.frame.DataFrame) -> None:
    try:
        conn = mysql.connector.connect(**db_config)

        if conn.is_connected():
            cursor = conn.cursor()

            # Prepare SQL query to INSERT a record into the database.
            insert_query = """
                INSERT INTO tyk_analytics_data (RunDate, APIKey, Plan, APIName, APIID, Host, Path, ResponseCode, Day, Month, TimeStamp, OrgID)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

            # Get today's date in 'YYYY-MM-DD' format
            today_date = datetime.now().date()

            # Iterate over DataFrame rows and execute insert query for each row
            for index, row in data_frame.iterrows():
                cursor.execute(insert_query, (
                    today_date,
                    row['APIKey'],
                    row['Plan'],
                    row['APIName'],
                    row['APIID'],
                    row['Host'],
                    row['Path'],
                    row['ResponseCode'],
                    row['Day'],
                    row['Month'],
                    row['TimeStamp'],
                    row['OrgID']
                ))

            # Commit the transaction
            conn.commit()
            print(f"{len(data_frame)} records inserted successfully.")

    except Exception as error:
        print(f"Error inserting records: {error}")
    finally:
        # Closing cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    data = {
        "APIKey": ["k1", "k2"],
        "Plan": ["p1", "p2"],
        "APIName": ["name1", "name2"],
        "APIID": ["id1", "id2"],
        "Host": ["www.example.com", "www.example.org"],
        "Path": ["/posts", "/comments"],
        "ResponseCode": ["200", "404"],
        "Day": [26, 27],
        "Month": [11, 11],
        "TimeStamp": [1732608151, 1732608152],
        "OrgID": ["FleetStudio", "FleetStudio"]
    }

    df = pd.DataFrame(data)
    write_analytics_data(df)