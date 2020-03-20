import sys, requests
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians

from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import col, radians
spark = SparkSession.builder.appName('events distance')\
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext # make sure we have Spark 2.3+

# main logic starts here
def main():

        # read the bike transaction data and events data into data frames
        events = spark.read.format('csv').options(header='true',inferSchema=True).load('events_latlng.csv').dropDuplicates()

        events = events.withColumnRenamed('Event ID','event_id').withColumnRenamed('Event Name','event_name').withColumnRenamed('Start Date/Time','start_date_time').withColumnRenamed('End Date/Time','end_date_time').withColumnRenamed('Event Agency','event_agency').withColumnRenamed('Event Type','event_type')
        events = events.withColumnRenamed('Event Borough','event_borough').withColumnRenamed('Event Location','event_location')

        events = events.select("event_id","event_name","start_date_time","end_date_time","event_agency","event_type","event_borough","event_location","latitude","longitude")

        events.write.format("org.apache.spark.sql.cassandra") \
        .options(table='events', keyspace='bike_share_analytics').save()
        
        
        
if __name__ == '__main__':
        
        main()
