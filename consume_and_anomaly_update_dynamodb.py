import boto3
import time
import json
from decimal import Decimal

REGION = "eu-north-1"
KINESIS_STREAM = "IoT_Sensor_Stream"
SHARD_ID = "shardId-000000000002"

DDB_TABLE = "IoT_Sensor_anamoly"   # your table name

kinesis = boto3.client("kinesis", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(DDB_TABLE)

shard_iterator = kinesis.get_shard_iterator(
    StreamName=KINESIS_STREAM,
    ShardId=SHARD_ID,
    ShardIteratorType="LATEST",   # use "TRIM_HORIZON" if you want old data
)["ShardIterator"]

while True:
    resp = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
    shard_iterator = resp["NextShardIterator"]

    for r in resp.get("Records", []):
        readings = json.loads(r["Data"].decode("utf-8"), parse_float=Decimal)
        print(readings)

        # TODO: your anomaly logic here -> table.put_item(Item=readings)

    time.sleep(0.2)
