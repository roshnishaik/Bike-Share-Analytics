import sys
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
#spark = SparkSession.builder.appName('example code').getOrCreate()
spark = SparkSession.builder.appName('bike_staion_etl') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
#assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

import json,math
import requests
# add more functions as necessary
def distance(lat1, lon1,lat2,lon2):
    #lat1, lon1 = origin
    #lat2, lon2 = destination
    radius = 6371 # km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def main():
    # main logic starts here
    nyc_bike_station_schema = types.StructType([
    types.StructField('altitude', types.StringType()),
    types.StructField('availableBikes', types.LongType()),
    types.StructField('availableDocks', types.LongType()),
    types.StructField('city', types.StringType()),
    types.StructField('id', types.LongType()),
    types.StructField('landMark', types.StringType()),
    types.StructField('lastCommunicationTime', types.StringType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('location', types.StringType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('postalCode', types.StringType()),
    types.StructField('stAddress1', types.StringType()),
    types.StructField('stAddress2', types.StringType()),
    types.StructField('stationName', types.StringType()),
    types.StructField('statusKey', types.LongType()),
    types.StructField('statusValue', types.StringType()),
    types.StructField('testStation', types.BooleanType()),
    types.StructField('totalDocks', types.LongType())])



    r = requests.get("https://feeds.citibikenyc.com/stations/stations.json")

    df = json.loads(r.text)

    df_station = spark.createDataFrame(df['stationBeanList'],schema = nyc_bike_station_schema)

    
    #reading weather station data from cassandra
    df_weather_station_data = spark.read.format("org.apache.spark.sql.cassandra").options(table='weather_station_data', keyspace="bike_share_analytics").load()
    df_weather_station_data1 = df_weather_station_data.select(*(functions.col(x).alias('weather_station_'+ x) for x in df_weather_station_data.columns))

    #joining station and weather data
    df_combined = df_station.crossJoin(df_weather_station_data1)

    #calculate distance from lat lon
    udf_get_distance = functions.udf(distance)
    df_combined = df_combined.withColumn("distance",udf_get_distance(df_combined.latitude,df_combined.longitude,df_combined.weather_station_latitude,df_combined.weather_station_longitude)).cache()

    #Groupby station id to get minimum distance weather station
    df_combined1 = df_combined.groupBy("id").agg(functions.min("distance").alias("min_distance"))
    df_combined1 =df_combined1.selectExpr("id as combined_station_id","min_distance")

    #join to previous combined data
    cond = [df_combined.id == df_combined1.combined_station_id, df_combined.distance == df_combined1.min_distance]
    df_combined_id = df_combined.join(df_combined1,cond,how="inner")


    #final dataframe to push to cassandra
    final_station_df = df_combined_id.select("id","altitude","availableBikes","availableDocks","city","landMark","lastCommunicationTime","latitude","longitude","location",
        "postalCode","stAddress1","stAddress2","stationName","statusKey","statusValue","testStation","totalDocks","weather_station_id")

    final_station_df = final_station_df.withColumnRenamed("availableBikes","available_bikes").withColumnRenamed("availableDocks","available_docks").withColumnRenamed("landMark","landmark").\
    withColumnRenamed("lastCommunicationTime","last_communication_time").withColumnRenamed("postalCode","postal_code").withColumnRenamed("stAddress1","st_address1").\
    withColumnRenamed("stAddress2","st_address2").withColumnRenamed("stationName","station_name").withColumnRenamed("statusKey","status_key").\
    withColumnRenamed("statusValue","status_value").withColumnRenamed("testStation","test_station").withColumnRenamed("totalDocks","total_docks")

    

    #push to cassandra
    final_station_df.write.format("org.apache.spark.sql.cassandra") \
    .options(table='station_data', keyspace='bike_share_analytics').save()


if __name__ == '__main__':
	
	main()

    