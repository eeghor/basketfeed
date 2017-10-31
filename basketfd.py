import boto3

client = boto3.client('kinesis')

st_desc = client.describe_stream(StreamName='utp-sales')

st_name = st_desc['StreamDescription']['StreamName']
st_shards = st_desc['StreamDescription']['Shards']   # list of shards (each is a dict)

print('shards in the feed: {}'.format(len(st_shards)))
for sh in st_shards:
	#print(sh['ShardId'])
	sh_it = client.get_shard_iterator(StreamName='utp-sales', ShardId=sh['ShardId'], ShardIteratorType='TRIM_HORIZON')
	#print(sh_it['ShardIterator'])
	sh_recs = client.get_records(ShardIterator=sh_it['ShardIterator'])  # records from shard
	print(sh_recs["Records"])