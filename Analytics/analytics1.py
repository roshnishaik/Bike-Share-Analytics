import sys
import uuid
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main():
    transaction_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load().cache()

    
    transaction_df.createOrReplaceTempView('transaction_df')

    #total number of trips in 2019
    total_trips=spark.sql('SELECT count(*) AS total_bikes FROM transaction_df')

    total_trips.write.format("org.apache.spark.sql.cassandra") \
    .options(table='total_trips', keyspace='bike_share_analytics').save()

    

    #number of distinct bikes
    distinct_bikes=spark.sql('SELECT COUNT(DISTINCT(bikeid)) AS number_of_bikes FROM transaction_df')
    distinct_bikes.write.format("org.apache.spark.sql.cassandra") \
    .options(table='distinct_bikes', keyspace='bike_share_analytics').save()
    

    #top 20 stations for starting trips

    start_trip_top_20 = transaction_df.groupBy(['start station name','start station latitude','start station longitude']).count()
    start_trip_top_20 = start_trip_top_20.withColumnRenamed('count','number_of_trips')
    start_trip_top_20 = start_trip_top_20.orderBy(start_trip_top_20.number_of_trips.desc()).limit(20)
               
    start_trip_top_20.write.format("org.apache.spark.sql.cassandra") \
    .options(table='start_trip_top_20', keyspace='bike_share_analytics').save()
    

    #average number of trips based on usertype
    average_trip_user_type=spark.sql('SELECT usertype, COUNT(*) AS number_of_trips, AVG(tripduration) AS average_duration \
                                      FROM transaction_df \
                                      WHERE usertype IN ("Subscriber","Customer") \
                                      GROUP BY(usertype)')
    average_trip_user_type.write.format("org.apache.spark.sql.cassandra") \
    .options(table='average_trip_user_type', keyspace='bike_share_analytics').save()
    

    # top 20 trips based on average_trip_user_type
    start_to_end_top_20=spark.sql('SELECT first(usertype) AS usertype,CONCAT(first(`start station name`)," to ",first(`end station name`)) AS route, COUNT(*) AS number_of_trips,AVG(tripduration) AS average_duration \
                                   FROM transaction_df \
                                   GROUP BY(`start station name`,`end station name`,usertype) \
                                   ORDER BY number_of_trips desc \
                                   LIMIT 20')
    start_to_end_top_20.write.format("org.apache.spark.sql.cassandra") \
    .options(table='start_to_end_top_20', keyspace='bike_share_analytics').save()

    #top 20 stations for ending trips

    end_trip_top_20 = transaction_df.groupBy(['end station name','end station latitude','end station longitude']).count()
    end_trip_top_20 = end_trip_top_20.withColumnRenamed('count','number_of_trips')
    end_trip_top_20 = end_trip_top_20.orderBy(end_trip_top_20.number_of_trips.desc()).limit(20)
    end_trip_top_20.write.format("org.apache.spark.sql.cassandra") \
    .options(table='end_trip_top_20', keyspace='bike_share_analytics').save()



if __name__ == '__main__':
    main()
