import sys, requests
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime

from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import year, to_date, col
spark = SparkSession.builder.appName('temp range').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext # make sure we have Spark 2.3+

# compute the latitudes and longitudes of an address
def geocodeAddress(streetaddress):
    
    streetaddress.replace(' ', '+')
    apikey = ''
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address='+streetaddress+'&key='+apikey
    
    req = requests.get(url)
    response = req.json()
    
    if response['results'] != []: 
        result = response['results'][0]
        geodata = str(result['geometry']['location']['lat']) + ',' + str(result['geometry']['location']['lng'])
    else:
        geodata = ''
    return geodata

# main logic starts here
def main(inputs, output):

        # read the input file into RDD with only two attributes hostname, number_of_bytes
        events = spark.read.format("csv").option("header", "true").load(inputs).dropDuplicates()
        events.createOrReplaceTempView('events')
        
        # select distinct event locations 
        distinct_event_loc = events.select(events["event location"]).distinct()
        distinct_event_loc.show()
        
	# call the geocoding api to get the latitudes and longitudes of the event location
        geocode_udf = functions.udf(geocodeAddress)
        geocode_events = distinct_event_loc.withColumn('geocode', geocode_udf(distinct_event_loc["event location"]))
        geocode_events = geocode_events.withColumn('latitude' , functions.split(geocode_events['geocode'],',').getItem(0))
        geocode_events = geocode_events.withColumn('longitude' , functions.split(geocode_events['geocode'],',').getItem(1))
        geocode_events.createOrReplaceTempView('geocode_events')
        
	# filter the events with empty latitude and longitude fields
        events_with_latlong = spark.sql("SELECT e.*, g.latitude, g.longitude FROM events e JOIN geocode_events g ON e.`Event Location` == g.`event location`")
        events_with_latlong = events_with_latlong.filter(events_with_latlong["geocode"] != '')
        events_with_latlong.show()
        events_with_latlong.coalesce(1).write.csv(output, header=True, mode='overwrite')
        
        
if __name__ == '__main__':
        inputs = sys.argv[1]
        output = sys.argv[2]
        main(inputs, output)
