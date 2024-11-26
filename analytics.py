import redis
import pandas as pd
import msgpack

def get_plan(api_key:str) -> str:

    plan = None
    #TODO: remove hard-coding hit api get-key/{api_key} and extract apply_policies from response and return it
    if api_key == "FleetStudioef16fdc2aeeb4984b2abe643f7d1cbf4":
        plan = "FreeDesign"
    elif api_key == "FleetStudio9b8f2bc601df46eeb6f94b1af5ba13b3":
        plan = "FreeDeveloper"
    return plan


def filter_by_date_range(data_frame: pd.DataFrame, date_range: list[int]) -> pd.DataFrame:
    """
    Filters the DataFrame for a custom date range using TimeStamp.

    Args:
        data_frame (pd.DataFrame): The input DataFrame containing analytics data.
        date_range (list): A list containing two long integers representing start and end timestamps.

    Returns:
        pd.DataFrame: A filtered DataFrame containing records within the specified date range.
    """
    start_timestamp = date_range[0]
    end_timestamp = date_range[1]

    # Ensure that TimeStamp is treated as an long for comparison
    return data_frame[data_frame['TimeStamp'].apply(lambda x: start_timestamp <= x[0] <= end_timestamp)]

def fetch_analytics_data() -> pd.DataFrame :
    redis_host = 'localhost'
    redis_port = 6379
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=False)

    analytics_key = 'analytics-tyk-system-analytics'

    # Fetch all data from the Redis list
    raw_data = redis_client.lrange(analytics_key, 0, -1)

    # List to hold all valid unpacked data
    analytics_records = []

    for encoded_data in raw_data:
        unpacked_data = msgpack.unpackb(encoded_data,raw=False)
        print("-" * 50)

        api_key = unpacked_data.get('APIKey')
        if not api_key:
            continue #skip invalid api keys

        # Extract only the required fields
        record = {
            "APIKey": api_key,
            "Plan": get_plan(api_key),
            "APIName": unpacked_data.get("APIName"),
            "APIID": unpacked_data.get("APIID"),
            "Host": unpacked_data.get("Host"),
            "Path": unpacked_data.get("Path"),
            "ResponseCode": unpacked_data.get("ResponseCode"),
            "Day": unpacked_data.get("Day"),
            "Month": unpacked_data.get("Month"),
            "TimeStamp": unpacked_data.get("TimeStamp"),
            "OrgID": unpacked_data.get("OrgID")
        }
        print(f"record={record}")

        # Append the valid record to the list
        analytics_records.append(record)

    return pd.DataFrame(analytics_records)

def main():
    # Fetch and process data
    df = fetch_analytics_data()
    print("df=\n", df)

    filtered_df = filter_by_date_range(df,[1732604550,1732605000])
    print("filtered_df=\n", filtered_df)

if __name__ == '__main__':
    main()

