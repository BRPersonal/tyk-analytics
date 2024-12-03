import os
from datetime import date,datetime
import mysql.connector
import psycopg2
from dotenv import load_dotenv
import json
import pandas as pd
from sqlalchemy import create_engine


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

def get_db_url() -> str :
    suffix = f"{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

    if db_config["port"] == "3306":
        print("Connecting to MySql")
        return "mysql+pymysql://" + suffix
    elif db_config["port"] == "5432":
        print("Connecting to PostGre")
        return "postgresql://" + suffix


def convert_to_date(timestamp : int) -> datetime.date:
    readable_time = datetime.fromtimestamp(timestamp)
    date_part = readable_time.date()
    return date_part

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
            INSERT INTO tyk_analytics_data (RunDate, APIKey, APIName, ResponseCode, RequestDate)
            VALUES (%s, %s, %s, %s, %s);
            """

        # Iterate over DataFrame rows and execute insert query for each row
        for row in analytics_records:
            cursor.execute(insert_query, (
                today_date,
                row['APIKey'],
                row['APIName'],
                row['ResponseCode'],
                convert_to_date(row['TimeStamp'])
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


def get_request_counts(db_url: str,
                       group_by_column_names: list[str],
                       start_date: date, end_date: date,
                       user_id: int | None) -> list[dict]:

    # Convert dates to string format for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    #Step 1: Create SQLAlchemy engine
    engine = create_engine(db_url)

    # Step 2: Execute the SQL query with dynamic date parameters
    if user_id:
        user_id_filter = f"and b.userId={user_id}"
    else:
        user_id_filter = ""

    query = f"""
    SELECT
        a.RequestDate as request_date,
        b.refApp as ref_app,
        b.userId as user_id,
        COUNT(*) AS cntr
    FROM tyk_analytics_data a, key_tbl b
    WHERE a.RequestDate BETWEEN '{start_date_str}' AND '{end_date_str}'
    {user_id_filter}
    and b.value = a.APIKey
    GROUP BY a.RequestDate, b.refApp, b.userId;
    """

    # Fetching data into a DataFrame
    df = pd.read_sql_query(query, engine)

    if df.empty:
        print("No records found")
        return None

    # Step 2: Group by one or more columns and sum 'cntr'.
    grouped_df = df.groupby(group_by_column_names, as_index=False)['cntr'].sum()

    # Step 3: Convert to list of dictionaries. rename cntr as Count
    result = grouped_df.rename(columns={'cntr': 'Count'}).to_dict(orient='records')

    return result


def fetch_and_group_by_column() -> None:
    start_date = date(2024, 12, 1)
    end_date = date(2024, 12, 2)

    #group_by_column_names = ["request_date"]
    #group_by_column_names = ["request_date", "ref_app"]
    group_by_column_names = ["ref_app", "user_id"]
    result = get_request_counts(db_url=get_db_url(),
                                group_by_column_names=group_by_column_names,
                                start_date=start_date,
                                end_date=end_date,
                                user_id = 3)
    print(result)



if __name__ == "__main__":
    fetch_and_group_by_column()