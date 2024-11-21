import pandas.core.frame
import redis
import pandas as pd
import json

def fetch_analytics_data() -> pandas.core.frame.DataFrame :
    redis_host = 'localhost'
    redis_port = 6379
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=False)

    analytics_key = 'analytics-tyk-system-analytics'

    # Fetch all data from the Redis list
    raw_data = redis_client.lrange(analytics_key, 0, -1)

    # Parse JSON data
    analytics_data = [json.loads(item) for item in raw_data]

    # Convert to DataFrame for analysis
    df = pd.DataFrame(analytics_data)
    print("type of df=", type(df))

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

