import boto3
import time
import json


client = boto3.client('kinesis')
shardIterator = client.get_shard_iterator(
    StreamName='BSM_Data_Stream1',
    ShardId='shardId-000000000000',
    ShardIteratorType='LATEST',
)['ShardIterator']


while True:
    response = client.get_records(
        ShardIterator=shardIterator
    )
    shardIterator = response['NextShardIterator']
    if len(response['Records']) > 0:
        for item in response['Records']:
            readings = json.loads(item["Data"])
        print(readings)

    time.sleep(0.2)
