import pandas as pd
import mysql.connector
from mysql.connector import Error
import psycopg2
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


def write_analytics_data(data_frame:pd.core.frame.DataFrame) -> None:
    try:
        if (db_config["port"] == "3306"):
            connection = mysql.connector.connect(**db_config)
            print("writing to MySql")
        elif (db_config["port"] == "5432"):
            connection = psycopg2.connect(**db_config)
            print("writing to PostGre")

        cursor = connection.cursor()

        # Prepare SQL query to INSERT a record into the database.
        insert_query = """
            INSERT INTO tyk_analytics_data (RunDate, APIKey, APIName, APIID, Host, Path, ResponseCode, Day, Month, TimeStamp, OrgID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().date()

        # Iterate over DataFrame rows and execute insert query for each row
        for index, row in data_frame.iterrows():
            cursor.execute(insert_query, (
                today_date,
                row['APIKey'],
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
        connection.commit()
        print(f"{len(data_frame)} records inserted successfully.")

    except Exception as error:
        print(f"Error inserting records: {error}")
    finally:
        # Closing cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            print("Database connection is closed.")

if __name__ == "__main__":
    data = {
        "APIKey": ["k1", "k2","k3","k4","k5"],
        "APIName": ["name1", "name2","name3","name4","name5"],
        "APIID": ["id1", "id2","id3","id4","id5"],
        "Host": ["www.example.com", "www.example.org","www.http-bin.org","www.http-bin.org","www.http-bin.org"],
        "Path": ["/posts", "/comments","/get","/delete","/put"],
        "ResponseCode": ["200", "404","200","200","200"],
        "Day": [26, 26,27,27,28],
        "Month": [11, 11,11,11,11],
        "TimeStamp": [1732608151, 1732608152,1732694551,1732694552,1732780951],
        "OrgID": ["FleetStudio", "FleetStudio","FleetStudio","FleetStudio","FleetStudio"]
    }

    df = pd.DataFrame(data)
    write_analytics_data(df)