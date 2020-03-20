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


def main(inputs):
    # main logic starts here
    nyc_bike_schema = types.StructType([
    types.StructField('tripduration', types.IntegerType()),
    types.StructField('starttime', types.TimestampType()),
    types.StructField('stoptime', types.TimestampType()),
    types.StructField('start station id', types.IntegerType()),
    types.StructField('start station name', types.StringType()),
    types.StructField('start station latitude', types.FloatType()),
    types.StructField('start station longitude', types.FloatType()),
    types.StructField('end station id', types.IntegerType()),
    types.StructField('end station name', types.StringType()),
    types.StructField('end station latitude', types.FloatType()),
    types.StructField('end station longitude', types.FloatType()),
    types.StructField('bikeid', types.IntegerType()),
    types.StructField('usertype', types.StringType()),
    types.StructField('birth year', types.IntegerType()),
    types.StructField('gender', types.IntegerType())])


    data = spark.read.option("header","true").csv(inputs,schema=nyc_bike_schema,sep=",")
    data = data.dropna()


    data=data.withColumn('startmonth',functions.month(data['starttime'])).withColumn('starthour',functions.hour(data['starttime']))
    data=data.withColumn('weekday',weekday(functions.dayofweek(data['starttime'])))
    data=data.withColumn('weekend',weekend(functions.dayofweek(data['starttime'])))
    data=data.withColumn('stopmonth',functions.month(data['stoptime'])).withColumn('stophour',functions.hour(data['stoptime']))
    data=data.withColumn('age',2019-data['birth year'])

   #data.show(5)

    data.write.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data2', keyspace='bike_share_analytics').save()


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)