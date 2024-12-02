import os
from datetime import date,datetime
import mysql.connector
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


def write_analytics_data(analytics_records:list[dict]) -> None:
    try:
        if db_config["port"] == "3306":
            connection = mysql.connector.connect(**db_config)
            print("writing to MySql")
        elif db_config["port"] == "5432":
            connection = psycopg2.connect(**db_config)
            print("writing to PostGre")

        cursor = connection.cursor()

        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().date()

        # Prepare SQL query to DELETE records for today's date
        delete_query =  """
                            DELETE FROM tyk_analytics_data WHERE RunDate = %s;
                        """

        # Execute the delete query
        cursor.execute(delete_query, (today_date,)) #to make it a sequence you need extra comma
        print(f"Deleted records for RunDate: {today_date}")

        # Prepare SQL query to INSERT a record into the database.
        insert_query = """
            INSERT INTO tyk_analytics_data (RunDate, APIKey, APIName, APIID, Host, Path, ResponseCode, Day, Month, TimeStamp, OrgID)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """


        # Iterate over DataFrame rows and execute insert query for each row
        for row in analytics_records:
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
        print(f"{len(analytics_records)} records inserted successfully.")

    except Exception as error:
        print(f"Error inserting records: {error}")
        raise error
    finally:
        # Closing cursor and connection
        if cursor:
            cursor.close()
            print("cursor is closed.")
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
            SELECT 
                a.RunDate, a.APIKey, a.APIName, a.APIID, a.ResponseCode,
                a.Day, a.Month, a.TimeStamp,
                b.userId, b.tier, b.refApp
            FROM tyk_analytics_data a, key_tbl b
            WHERE b.value = a.APIKey
            ORDER BY b.userId DESC
            """

            # Execute the query
            cursor.execute(sql_query)

            # Fetch all results
            results = cursor.fetchall()

            #check if resultset is empty
            if not results:
                print("No records returned from the query")
                return None

            # Get list of column names from cursor description which is a list of tuples.
            #First element of a tuple is the columnName
            column_names = [desc[0] for desc in cursor.description]

            # Construct the dictionary grouped by given column
            result_dict = {"groupBy" : group_by_column}

            for row in results:
                entry = dict(zip(column_names, row))  # Create a dictionary using column names
                group_by_value = entry[group_by_column]  # Accessing 'grouo by' using field name

                if group_by_value not in result_dict:
                    result_dict[group_by_value] = []

                result_dict[group_by_value].append(entry)

            return result_dict

    finally:
        print("closing connection")
        connection.close()


def test_group_by() -> None:
    #result = fetch_and_group_by_column("tier")
    result = fetch_and_group_by_column("refApp")
    #result = fetch_and_group_by_column("userId")

    if result:
        json_result = dict_to_json_string(result)
        print(json_result)

if __name__ == "__main__":
    test_group_by()