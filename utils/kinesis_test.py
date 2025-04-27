import boto3

client = boto3.client('kinesis', region_name='eu-west-1')

response = client.describe_stream(StreamName='Bolt-Trip')
print(response['StreamDescription']['StreamStatus'])
