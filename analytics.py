import redis
import msgpack
from analytics_repository import write_analytics_data


def fetch_analytics_data() -> list[dict] :
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
            "APIName": unpacked_data.get("APIName"),
            "ResponseCode": unpacked_data.get("ResponseCode"),
            "TimeStamp": unpacked_data.get("TimeStamp")[0],   #first element alone is enough
        }
        print(f"record={record}")

        # Append the valid record to the list
        analytics_records.append(record)

    return analytics_records

def main():
    # Fetch and process data
    analytics_records = fetch_analytics_data()
    print(f"records to be inserted={len(analytics_records)}")

    if len(analytics_records) != 0:
        write_analytics_data(analytics_records)



if __name__ == '__main__':
    main()

