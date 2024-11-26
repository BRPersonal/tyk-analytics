import pandas.core.frame
import redis
import pandas as pd
import msgpack
import base64
import csv
from datetime import datetime
from io import StringIO

def fetch_analytics_data() -> pandas.core.frame.DataFrame :
    redis_host = 'localhost'
    redis_port = 6379
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=False)

    analytics_key = 'analytics-tyk-system-analytics'

    # Fetch all data from the Redis list
    raw_data = redis_client.lrange(analytics_key, 0, -1)

    only_first_time = True
    for encoded_data in raw_data:
        unpacked_data = msgpack.unpackb(encoded_data,raw=False)
        print("-" * 50)

        api_key = unpacked_data.get('APIKey')
        if not api_key:
            continue #skip invalid api keys

        print(f"apiKey={api_key}")
        print(f"apiName={unpacked_data.get('APIName')}")
        print(f"apiId={unpacked_data.get('APIID')}")
        print(f"host={unpacked_data.get('Host')}")
        print(f"path={unpacked_data.get('Path')}")
        print(f"responseCode={unpacked_data.get('ResponseCode')}")

        print(f"Day={unpacked_data.get('Day')}")
        print(f"Month={unpacked_data.get('Month')}")
        print(f"TimeStamp={unpacked_data.get('TimeStamp')}")
        print(f"OrgID={unpacked_data.get('OrgID')}")




    df = pd.DataFrame()
    return df

# Function to generate a simple report
def generate_report(df:pandas.core.frame.DataFrame):
    # # Example: Group by API endpoint and count requests
    # report = df.groupby('api_endpoint').size().reset_index(name='request_count')
    #
    # # Sort by request count
    # report = report.sort_values(by='request_count', ascending=False)
    print("df=\n", df)
    report = "Hare Krishna"

    return report

def main():
    # Fetch and process data
    df = fetch_analytics_data()

    # Generate report
    report = generate_report(df)

    print(report)

if __name__ == '__main__':
    main()

