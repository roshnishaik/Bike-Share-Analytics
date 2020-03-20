import sys
import uuid
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit
#cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main():
    transaction_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load()

    transaction_df.createOrReplaceTempView('transaction_df')

    #number of trips by age
    trips_age=spark.sql('SELECT age,COUNT(*) AS total_count, \
                         COUNT(case WHEN gender=1 THEN 1 END) AS males, \
                         COUNT(case WHEN gender=2 THEN 1 END) AS females, \
                         COUNT(case WHEN gender=0 THEN 1 END) AS unknown \
                         FROM transaction_df \
                         GROUP BY(age) \
                         ORDER BY(age)')
    
    trips_age.write.format("org.apache.spark.sql.cassandra") \
    .options(table='trips_age', keyspace='bike_share_analytics').save()


    #avergae trip duration by age
    trip_duration_age=spark.sql("SELECT age,AVG(tripduration) as avg_trip_duration \
                                 FROM transaction_df \
                                 GROUP BY(age)")
    
    trip_duration_age.write.format("org.apache.spark.sql.cassandra") \
    .options(table='trip_duration_age', keyspace='bike_share_analytics').save()



if __name__ == '__main__':
    main()
