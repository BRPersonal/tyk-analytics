import os
from datetime import date, datetime
import pandas as pd
from pandas.core.frame import DataFrame
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

def execute_sql_query(query : str) -> DataFrame:
    db_url = get_db_url()
    engine = create_engine(db_url)
    return pd.read_sql_query(query, engine)

def convert_to_date(timestamp : int) -> datetime.date:
    readable_time = datetime.fromtimestamp(timestamp)
    date_part = readable_time.date()
    return date_part

def write_analytics_data(analytics_records:list[dict]) -> None:
    try:

        # Create the SQLAlchemy engine
        db_url = get_db_url()
        engine = create_engine(db_url)

        # Get today's date in 'YYYY-MM-DD' format
        today_date = datetime.now().date()

        with engine.connect() as connection:
            # Prepare SQL query to DELETE records for today's date
            delete_query = text("""
                            DELETE FROM tyk_analytics_data WHERE run_date = :runDate;
                           """)

            # Execute the delete query
            connection.execute(delete_query, {'runDate': today_date})

            print(f"Deleted records for RunDate: {today_date}")

            # Prepare SQL query to INSERT a record into the database.
            insert_query = text("""
                INSERT INTO tyk_analytics_data (run_date, api_key, api_name, response_code, request_date)
                VALUES (:runDate, :apiKey, :apiName, :responseCode, :requestDate);
                """)

            # Iterate over DataFrame rows and execute insert query for each row
            for row in analytics_records:
                connection.execute(insert_query, {
                    'runDate': today_date,
                    'apiKey': row['APIKey'],
                    'apiName': row['APIName'],
                    'responseCode': row['ResponseCode'],
                    'requestDate': convert_to_date(row['TimeStamp'])
                })

            # Commit the transaction
            connection.commit()

        print(f"{len(analytics_records)} records inserted successfully.")

    except Exception as error:
        print(f"Error writing to analytics: {error}")
        raise error

def get_request_counts(
                       group_by_column_names: list[str],
                       start_date: date, end_date: date,
                       user_id: int | None) -> list[dict]:

    # Convert dates to string format for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    # Step 2: Execute the SQL query with dynamic date parameters
    if user_id:
        user_id_filter = f"and b.user_id={user_id}"
    else:
        user_id_filter = ""

    query = f"""
    SELECT
        a.request_date,
        b.ref_app,
        b.user_id,
        COUNT(*) AS cntr
    FROM tyk_analytics_data a, key_tbl b
    WHERE a.request_date BETWEEN '{start_date_str}' AND '{end_date_str}'
    {user_id_filter}
    and b.value = a.api_key
    GROUP BY a.request_date, b.ref_app, b.user_id;
    """

    # Fetch data from database into a DataFrame
    df = execute_sql_query(query)

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
    end_date = date(2024, 12, 3)

    #group_by_column_names = ["request_date"]
    group_by_column_names = ["request_date", "ref_app"]
    #group_by_column_names = ["ref_app", "user_id"]
    result = get_request_counts(group_by_column_names=group_by_column_names,
                                start_date=start_date,
                                end_date=end_date,
                                user_id = None)
    print(result)



if __name__ == "__main__":
    fetch_and_group_by_column()