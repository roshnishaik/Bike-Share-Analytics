import sys, requests
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians

from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import col, radians
spark = SparkSession.builder.appName('events distance').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext # make sure we have Spark 2.3+

# compute distance between two pairs of latitudes and longitudes
def compute_distance(lat1, lng1, lat2, lng2):
    
    # Transform to radians
    lat2 = float(lat2) * (math.pi/180.0)
    lng2 = float(lng2) * (math.pi/180.0)
    lng1 = float(lng1) * (math.pi/180.0)
    lat1 = float(lat1) * (math.pi/180.0)
    
    # compute difference
    lng_dist = lng2 - lng1
    lat_dist = lat2 - lat1
    
    # Calculate area
    area = sin(lat_dist/2)**2 + cos(lat1) * cos(lat2) * sin(lng_dist/2)**2
    #return area
    
    # Calculate the central angle
    central_angle = 2 * atan2(sqrt(area), sqrt(1 - area))
    radius = 6371
    
    # Calculate Distance
    distance = central_angle * radius
    return distance
    #return abs(round(distance, 2))
    

# main logic starts here
def main(biketrip_input, events_input, output):

        # define bike trip schema
        nyc_bike_schema = types.StructType([
            types.StructField('tripduration', types.IntegerType()),
            types.StructField('starttime', types.TimestampType()),
            types.StructField('stoptime', types.TimestampType()),
            types.StructField('start_station_id', types.IntegerType()),
            types.StructField('start_station_name', types.StringType()),
            types.StructField('start_station_latitude', types.FloatType()),
            types.StructField('start_station_longitude', types.FloatType()),
            types.StructField('end_station_id', types.IntegerType()),
            types.StructField('end_station_name', types.StringType()),
            types.StructField('end_station_latitude', types.FloatType()),
            types.StructField('end_station_longitude', types.FloatType()),
            types.StructField('bikeid', types.IntegerType()),
            types.StructField('usertype', types.StringType()),
            types.StructField('birth year', types.IntegerType()),
            types.StructField('gender', types.IntegerType())
        ])
        
        # define events schema
        event_schema = types.StructType([
            types.StructField('eventID', types.StringType()),
            types.StructField('event_name', types.StringType()),
            types.StructField('startDateTime', types.TimestampType()),
            types.StructField('endDateTime', types.TimestampType()),
            types.StructField('event_agency', types.StringType()),
            types.StructField('event_type', types.StringType()),
            types.StructField('event_borough', types.StringType()),
            types.StructField('event_location', types.StringType()),
            types.StructField('latitude', types.StringType()),
            types.StructField('longitude', types.StringType())
        ])

        # read the bike transaction data and events data into data frames
        events = spark.read.format("csv").option("header", "true").load(events_input).dropDuplicates()
        bikedata = spark.read.format("csv").option("header", "true").load(biketrip_input)
        
        # find the stations which have highest inflow of bikes at a particular point of time
        freq_stations = bikedata.groupBy(bikedata['end station latitude'], bikedata['end station longitude'], functions.to_date(bikedata["starttime"]).alias("date"), functions.hour(bikedata["starttime"]).alias("hour"), bikedata['end station id'].alias("stationid")).count()     
        freq_stations = freq_stations.filter(freq_stations["count"] >= 150)   
        
        station_event = freq_stations.crossJoin(events.select(events["latitude"], events["longitude"], events["Event ID"], events["Event Name"]))
        
	# compute the distance between events' coordinates and station coordinates
        compute_dist_udf = functions.udf(compute_distance)
        station_event = station_event.withColumn('distance', compute_dist_udf(station_event['end station latitude'], station_event['end station longitude'], station_event['latitude'], station_event['longitude'])).dropDuplicates()
        station_event = station_event.filter(station_event['distance'] < 4)
        
        station_event.coalesce(1).write.csv(output, header=True, mode='overwrite')
        
        
if __name__ == '__main__':
        biketrip_input = sys.argv[1]
        events_input = sys.argv[2]
        main(biketrip_input, events_input, output)

