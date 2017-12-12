import boto3
import gzip
import io
import json
import time 
import sys
import arrow

class BasketFeedCatcher:

	'''
	watch basketball feed and grab new transactions in real time
	'''

	def __init__(self):

		self.BASKETFEED_NAME = 'utp-sales'
		self.BASKETFEED_S3_BUCKET = 'transaction-raw-tega'
		self.S3_CREDENTIALS = json.load(open(sys.argv[1], 'r'))

	def _set_time(self):

		right_now = arrow.now('Australia/Sydney')
		
		self.THIS_HOUR = (right_now.floor('hour'), right_now.ceil('hour'))

		self.JSON_FILE_NAME = '-'.join(['feed', right_now.format('YYYY-MM-DD'), right_now.format('HH'), '00', right_now.format('HH'), '59.json'])

		return self

	def __repr__(self):

		return f'BasketFeedCatcher({self.BASKETFEED_NAME!r} -> {self.self.BASKETFEED_S3_BUCKET!r})'

	def connect_to_s3(self):

		self.client_s3 = boto3.client('s3', **self.S3_CREDENTIALS)

		try:
			response = self.client_s3.head_bucket(Bucket=self.BASKETFEED_S3_BUCKET) 
			'''
			{'ResponseMetadata': 
				{'RequestId': '815746FDEBFDEB82', 'HostId': 'RewdWNJOH9opzJCQZcaPg4RRxs4XXneePCdK0JK1SUABCbEewK561LOO8WTnrRt8a7bBAlbyCEI=', 
				'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': 'RewdWNJOH9opzJCQZcaPg4RRxs4XXneePCdK0JK1SUABCbEewK561LOO8WTnrRt8a7bBAlbyCEI=', 
				'x-amz-request-id': '815746FDEBFDEB82', 'date': 'Tue, 12 Dec 2017 01:56:14 GMT', 
				'x-amz-bucket-region': 'ap-southeast-2', 'content-type': 'application/xml', 
				'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 0}}

			if anything returned successfully the bucket is fine; else an exception is being thrown
			'''
		except:
			raise Exception(f'you can\'t acces your s3 bucket {self.BASKETFEED_S3_BUCKET}!')

		return self

	def connect_to_feed(self):

		self.client_kinesis = boto3.Session().client('kinesis')

		st_desc = self.client_kinesis.describe_stream(StreamName=self.BASKETFEED_NAME)

		'''
		{'StreamDescription': {'StreamName': 'utp-sales', 
								'StreamARN': 'arn:aws:kinesis:ap-southeast-2:389920326251:stream/utp-sales', 
								'StreamStatus': 'ACTIVE', 
								'Shards': [{'ShardId': 'shardId-000000000000', 'HashKeyRange': 
											{'StartingHashKey': '0', 
											'EndingHashKey': '340282366920938463463374607431768211455'}, 
											'SequenceNumberRange': {'StartingSequenceNumber': '49578281082663149167125614694060318974137900028592652290'}}], 
											'HasMoreShards': False, 'RetentionPeriodHours': 24, 
											'StreamCreationTimestamp': datetime.datetime(2017, 10, 26, 13, 27, 1, tzinfo=tzlocal()), 
											'EnhancedMonitoring': [{'ShardLevelMetrics': []}], 'EncryptionType': 'NONE'}, 
											'ResponseMetadata': {'RequestId': 'fe25a6f5-8ad8-a6d3-ac81-85a40bad3323', 
											'HTTPStatusCode': 200, 
											'HTTPHeaders': {'server': 'Apache-Coyote/1.1', 
											'x-amzn-requestid': 'fe25a6f5-8ad8-a6d3-ac81-85a40bad3323', 'x-amz-id-2': 
											'pP1BAhqHU4+UAENgb9c3E66XTy/nCyx2ODbYog/tzfOSXQl3+aAjsWQGDaeUh0ut0AgyahYQWibIo1fExnU8af+Evmazhn3e', 
											'content-type': 'application/x-amz-json-1.1', 
											'content-length': '558', 'date': 'Fri, 08 Dec 2017 05:20:16 GMT'}, 'RetryAttempts': 0}}
		'''

		self.shard_ids = [s['ShardId'] for s in st_desc['StreamDescription']['Shards']]  # list, probably just 1 element

		return self

	def catch_records(self):

		self._set_time()

		shard_it = self.client_kinesis.get_shard_iterator(StreamName=self.BASKETFEED_NAME, ShardId=self.shard_ids[0], ShardIteratorType='LATEST')

		while 1:
		
			collected_recs = []
		
			next_shit = shard_it['ShardIterator']
		
			while next_shit:
				
				resp = self.client_kinesis.get_records(ShardIterator=next_shit)
				
				# pause to avoid the 'rate exceeded' error; shard has a read throughput of up to 5 transactions per sec for reads
				time.sleep(0.5)

				recs = resp['Records']
		
				if recs:  # if non-empty

					current_time = arrow.now('Australia/Sydney')

					if (self.THIS_HOUR[0] <= current_time) and (current_time <= self.THIS_HOUR[1]):  # note: right end NOT included

						for r in recs:
	
							r_txt = json.loads(gzip.GzipFile(fileobj=io.BytesIO(r[u'Data']), mode='rb').read().decode())  # this is a dict now
							
							collected_recs.append(r_txt)

						self.client_s3.put_object(Body=json.dumps(collected_recs), Bucket=self.BASKETFEED_S3_BUCKET, Key=self.JSON_FILE_NAME)

						next_shit = resp['NextShardIterator']

					else:

						json.dump(collected_recs, open(self.JSON_FILE_NAME, 'w'))

						self.client_s3.upload_fileobj(io.StringIO(json.dumps(collected_recs)), self.BASKETFEED_S3_BUCKET, self.JSON_FILE_NAME)

						collected_recs.clear()

						self._set_time()				

				else:

					next_shit = resp['NextShardIterator']

if __name__ == '__main__':

	c = BasketFeedCatcher().connect_to_s3().connect_to_feed().catch_records()