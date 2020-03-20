import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code')\
.config('spark.cassandra.connection.host','127.0.0.1').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

# add more functions as necessary
@functions.udf(returnType=types.IntegerType())
def weekday(dayofweek):
    if dayofweek<=6 and dayofweek!=1:
        return 1
    else:
        return 0

@functions.udf(returnType=types.IntegerType())
def weekend(dayofweek):
    if dayofweek==1 or dayofweek==7:
        return 1
    else:
        return 0


def main():
    # main logic starts here
    
    #Read bike_trips_data
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table='transaction_data2', keyspace="bike_share_analytics").load()
    data = data.dropna()

    #read bike station data
    df_station = spark.read.format("org.apache.spark.sql.cassandra").options(table='station_data', keyspace="bike_share_analytics").load()
    df_station = df_station.select("id","weather_station_id")


    #join data
    cond = [data['start station id']==df_station.id]
    df_combined = data.join(df_station,cond,how='inner')

    df_combined = df_combined.select('tripduration','starttime','stoptime','start station id',
        'start station name','start station latitude','start station longitude','end station id','end station name',
        'end station latitude','end station longitude','bikeid','usertype','birth year','gender','weather_station_id')


    #Create new columns
    df_combined=df_combined.withColumn('startyear',functions.year(df_combined['starttime']))
    df_combined=df_combined.withColumn('dayofyear',functions.dayofyear(df_combined['starttime']))




    ##Adding weather data to trips data
    df_weather = spark.read.format("org.apache.spark.sql.cassandra").options(table='weather_data1', keyspace="bike_share_analytics").load()


    df_weather = df_weather.withColumn("timestamp",functions.unix_timestamp('time', "yyyy-MM-dd HH:mm:ss").cast(types.TimestampType()))

    df_weather = df_weather.withColumn('hour_weather',functions.hour(df_weather['timestamp']))
    df_weather = df_weather.withColumn('month_weather',functions.month(df_weather['timestamp']))
    df_weather = df_weather.withColumn('year_weather',functions.year(df_weather['timestamp']))
    df_weather = df_weather.withColumn('dayofyear_weather',functions.dayofyear(df_weather['timestamp']))

    
    cond1 = [df_combined.weather_station_id == df_weather.id,
            df_combined.startyear == df_weather.year_weather,
            df_combined.dayofyear == df_weather.dayofyear_weather,
            df_combined.starthour == df_weather.hour_weather]

    df_combined_new = df_combined.join(df_weather,cond1,how="inner")

    df_combined_new = df_combined_new.select('tripduration','starttime','stoptime','start station id',
         'start station name','start station latitude','start station longitude','end station id','end station name',
         'end station latitude','end station longitude','bikeid','usertype','birth year','gender','age','dayofyear','starthour',
         'startmonth','startyear','stophour','stopmonth','weekday','weekend','temperature','precipitation','humidity','dewpoint','windspeed')
   
    #pushing data to cassandra
    df_combined_new.write.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').save()


if __name__ == '__main__':
    
    main()