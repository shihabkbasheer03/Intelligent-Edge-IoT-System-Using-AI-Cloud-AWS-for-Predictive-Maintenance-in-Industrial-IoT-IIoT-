import boto3
import time
import json

REGION = "eu-north-1"
STREAM_NAME = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"   # or 000000000000 / 000000000001

client = boto3.client("kinesis", region_name=REGION)

shardIterator = client.get_shard_iterator(
    StreamName=STREAM_NAME,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",   # or "TRIM_HORIZON"
)["ShardIterator"]

while True:
    response = client.get_records(ShardIterator=shardIterator, Limit=100)
    shardIterator = response["NextShardIterator"]

    for item in response.get("Records", []):
        readings = json.loads(item["Data"].decode("utf-8"))  # Data is bytes
        print(readings)

    time.sleep(0.2)
