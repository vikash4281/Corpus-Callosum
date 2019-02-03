import json
import boto3
from smart_open import smart_open, codecs
import confluent_kafka
from kafka.producer import KafkaProducer
from kafka.client import KafkaClient

def publish_message(producerInstance, topic_name, key, value):
    "Function to send messages to the specific topic"
    try:
        producerInstance.produce(topic_name,key=key,value=value)
        producerInstance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    "Function to create a producer handle"
    _producer = None
    conf = {'bootstrap.servers': 'ec2-35-165-113-215.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-54-69-173-183.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-54-218-166-98.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-52-24-63-41.us-west-2.compute.amazonaws.com:9092'}
    try:
        _producer = confluent_kafka.Producer(conf)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


client = boto3.client('s3')
resource = boto3.resource('s3')
my_bucket = resource.Bucket('gdelt-sample-data')
files = list(my_bucket.objects.filter(Prefix='export'))
obj = codecs.getreader('utf-8')(files[0].get()['Body'])
events_columns = ['GlobalEventID', 'Day', 'MonthYear', 'Year', 'FractionDate',
                  'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
                  'Actor1KnownGroupCode', 'Actor1EthnicCode',
                  'Actor1Religion1Code', 'Actor1Religion2Code',
                  'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
                  'Actor2Code', 'Actor2Name', 'Actor2CountryCode',
                  'Actor2KnownGroupCode', 'Actor2EthnicCode',
                  'Actor2Religion1Code', 'Actor2Religion2Code',
                  'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
                  'IsRootEvent', 'EventCode', 'EventBaseCode',
                  'EventRootCode', 'QuadClass', 'GoldsteinScale',
                  'NumMentions', 'NumSources', 'NumArticles', 'AvgTone',
                  'Actor1Geo_Type', 'Actor1Geo_Fullname',
                  'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
                  'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
                  'Actor1Geo_FeatureID', 'Actor2Geo_Type',
                  'Actor2Geo_Fullname', 'Actor2Geo_CountryCode',
                  'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code',
                  'Actor2Geo_Lat', 'Actor2Geo_Long', 'Actor2Geo_FeatureID',
                  'ActionGeo_Type', 'ActionGeo_Fullname',
                  'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
                  'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long',
                  'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']
producer = KafkaProducer(bootstrap_servers= 'ec2-35-165-113-215.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-54-69-173-183.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-54-218-166-98.us-west-2.compute.amazonaws.com:9092,'
                                 'ec2-52-24-63-41.us-west-2.compute.amazonaws.com:9092')

kafkaProducer=connect_kafka_producer()
for record in obj:
    features = record.strip().split("\t")
    tempDict = dict({events_columns[i]:features[i].encode("utf-8") for i in range(len(events_columns))})

publish_message(kafkaProducer,"my-topic","events",json.dumps(tempDict))
   # producer.send("my-topic",str(record.encode("utf-8")),"events")


# def get_file_handle(myBucket,fileName):
#     fileHandle = myBucket.objects(key = fileName)
#
# def get_all_bucket_files(myBucket):
#     fileHandleList = []
#     for object in myBucket.objects.all():
#         fileHandleList.append(get_file_handle(myBucket,object.key))
#     return fileHandleList
#
#
# def get_bucket_details(bucketName):
#     s3 = boto3.resource('s3')
#     my_bucket = s3.Bucket(bucketName)
#     return my_bucket

# if __name__ == '__main__':
#     bucketName = "gdelt-sample-data"
#     #topicName = sys.argv[2]
#     for line in smart_open('s3://gdelt-sample-data/export.csv'):
#         print(line)
