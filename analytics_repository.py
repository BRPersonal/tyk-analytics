import os
from datetime import date,datetime
import mysql.connector
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import json


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
        if db_config["port"] == "3306":
            connection = mysql.connector.connect(**db_config)
            print("writing to MySql")
        elif db_config["port"] == "5432":
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

def dict_to_json_string(data_dict:dict) -> str:
    def custom_date_serializer(obj):
        if isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')  # format date as YYYY-mm-dd
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

    return json.dumps(data_dict, indent=4,default=custom_date_serializer) #convert to json with pretty printing

def fetch_and_group_by_column(group_by_column: str) -> dict:
    try:
        if db_config["port"] == "3306":
            connection = mysql.connector.connect(**db_config)
            print("Reading from MySql")
        elif db_config["port"] == "5432":
            connection = psycopg2.connect(**db_config)
            print("Reading from PostGre")

        with connection.cursor() as cursor:
            # Define the SQL query
            sql_query = """
            SELECT DISTINCT
                a.RunDate, a.APIKey, a.APIName, a.APIID, a.ResponseCode,
                a.Day, a.Month, a.TimeStamp,
                b.userId, b.tier, b.refApp
            FROM tyk_analytics_data a, key_tbl b
            where b.value = a.APIKey;
            """

            # Execute the query
            cursor.execute(sql_query)

            # Fetch all results
            results = cursor.fetchall()

            # Get list of column names from cursor description which is a list of tuples.
            #First element of a tuple is the columnName
            column_names = [desc[0] for desc in cursor.description]

            # Construct the dictionary grouped by tier
            result_dict = {"groupBy" : group_by_column}

            for row in results:
                entry = dict(zip(column_names, row))  # Create a dictionary using column names
                group_by_value = entry[group_by_column]  # Accessing 'grouo by' using field name

                if group_by_value not in result_dict:
                    result_dict[group_by_value] = []

                result_dict[group_by_value].append(entry)

            return result_dict

    finally:
        connection.close()

def test_insert() -> None:
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

def test_group_by() -> None:
    #result = fetch_and_group_by_column("tier")
    result = fetch_and_group_by_column("refApp")
    #result = fetch_and_group_by_column("userId")
    json_result = dict_to_json_string(result)
    print(json_result)

if __name__ == "__main__":
    test_group_by()