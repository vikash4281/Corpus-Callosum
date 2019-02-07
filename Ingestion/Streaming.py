import json
import boto3
from smart_open import smart_open, codecs
from ConfigParser import ConfigParser
import psycopg2

def publish_message(producerInstance, topic_name, key, value):
    "Function to send messages to the specific topic"
    try:
        producerInstance.produce(topic_name,key=key,value=value)
        producerInstance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def insert_data(finaldict,tablename):
    conn = None
    try:
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        curs = conn.cursor()
        query = curs.mogrify("INSERT INTO {} ({}) VALUES {}".format(
            tablename,
            ', '.join(finaldict[0].keys()),
            ', '.join(["%s"] * len(finaldict))
        ), [tuple(v.values()) for v in finaldict])
        print(query)
        curs.execute(query)
        conn.commit()
        curs.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# def connect_kafka_producer():
#     "Function to create a producer handle"
#     _producer = None
#     conf = {'bootstrap.servers': 'ec2-35-165-113-215.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-54-69-173-183.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-54-218-166-98.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-52-24-63-41.us-west-2.compute.amazonaws.com:9092'}
#     try:
#         _producer = confluent_kafka.Producer(conf)
#     except Exception as ex:
#         print('Exception while connecting Kafka')
#         print(str(ex))
#     finally:
#         return _producer

# producer = KafkaProducer(bootstrap_servers= 'ec2-35-165-113-215.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-54-69-173-183.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-54-218-166-98.us-west-2.compute.amazonaws.com:9092,'
#                                  'ec2-52-24-63-41.us-west-2.compute.amazonaws.com:9092')

#kafkaProducer=connect_kafka_producer()

def get_event_files(tableprefix):
    return list(my_bucket.objects.filter(Prefix=tableprefix))


client = boto3.client('s3')
resource = boto3.resource('s3')
my_bucket = resource.Bucket('gdelt-sample-data')
events_files = get_event_files("event")
gkg_files = get_event_files("gkg")
mentions_files = get_event_files("mentions")
gkg_obj = codecs.getreader('utf-8')(gkg_files[0].get()['Body'])
event_obj = codecs.getreader('utf-8')(events_files[0].get()['Body'])
mention_obj = codecs.getreader('utf-8')(mentions_files[0].get()['Body'])
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

gkg = ["recordid","date" , "srccollectionidentifier","srccommonname","documentid","counts","countsv1","themes","enhancedthemes",
	"locations", "enhancedlocation","persons","enhancedpersons","organizations","enhancedorganizations","tone","enhanceddates",
	"gcam","sharingimage","relatedimages", "socialimageembeds", "socialvideoembeds", "quotations", "allnames", "amounts","translationinfo",
	"extrasxml"]

mentions = ["GLOBALEVENTID","EventTimeDate","MentionTimeDate","MentionType","MentionSourceName","MentionIdentifier","SentenceID",
            "Actor1CharOffset","Actor2CharOffset","ActionCharOffset","InRawText","Confidence","MentionDocLen","MentionDocTone",
            "MentionDocTranslationInfo","Extras"]

gkg_finaldict=[]
for record in gkg_obj:
    features = record.strip().split("\t")
    if(len(features)==27):
        tmpDict = dict()
        tmpDict = dict({gkg[i]:features[i].encode("utf-8") for i in range(len(gkg))})
        gkg_finaldict.append(tmpDict)

for i in range(0,len(gkg_finaldict),1000):
    insert_data(gkg_finaldict[i:i+1000],"public.gkg")

event_finaldict=[]
for record in event_obj:
    features = record.strip.split("\t")
    if(len(features)==61):
        tmpDict = dict()
        tmpDict = dict({events_columns[i]: features[i].encode("utf-8") for i in range(len(events_columns))})
        event_finaldict.append(tmpDict)

for i in range(0,len(gkg_finaldict),1000):
    insert_data(gkg_finaldict[i:i+1000],"public.events")

mentions_finaldict=[]
for record in event_obj:
    features = record.strip.split("\t")
    if(len(features)==16):
        tmpDict = dict()
        tmpDict = dict({mentions_files[i]: features[i].encode("utf-8") for i in range(len(mentions))})
        event_finaldict.append(tmpDict)

for i in range(0,len(gkg_finaldict),1000):
    insert_data(gkg_finaldict[i:i+1000],"public.mentions")


#publish_message(kafkaProducer, "my-topic", "gkg", record)
   # features = record.strip().split("\t")
   # tempDict = dict({events_columns[i]:features[i].encode("utf-8") for i in range(len(events_columns))})


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
