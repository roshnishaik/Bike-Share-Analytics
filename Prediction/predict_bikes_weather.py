import sys
import uuid
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import DecisionTreeRegressor, GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


spark = SparkSession.builder.appName('citi analytics') \
    .config('spark.cassandra.connection.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main():
    
    transaction_df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='transaction_data5', keyspace='bike_share_analytics').load()

    transaction_df = transaction_df.filter(transaction_df.startyear >= 2017 )
    
    weather_transaction = transaction_df.withColumn('startdate', functions.to_date(transaction_df['starttime']))
    weather_transaction=weather_transaction.dropna()
    weather_transaction.createOrReplaceTempView('weather_transaction')

    weather_transaction = spark.sql("SELECT `start station id` ,`start station name`,starthour,startmonth,startdate,weekday,weekend,temperature,humidity,windspeed,COUNT(*) AS number_of_trips FROM weather_transaction GROUP BY `start station id`,`start station name`,starthour,startdate,startmonth,weekday,weekend,temperature,humidity,windspeed")
    weather_transaction.createOrReplaceTempView('weather_transaction')

    data = spark.sql("SELECT past.`start station id` AS sstid, past.starthour AS starthour, past.startdate AS startdate, past.startmonth AS startmonth, past.weekday AS weekday, past.weekend AS weekend,past.temperature AS temperature,past.humidity AS humidity, past.windspeed AS windspeed, past.number_of_trips AS past_trips, current.number_of_trips AS number_of_trips FROM weather_transaction past JOIN weather_transaction current ON past.`start station id` = current.`start station id` AND past.starthour = current.starthour - 1 AND past.startdate = current.startdate")
    data.createOrReplaceTempView('data')

    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()


    citi_assembler=VectorAssembler(inputCols=['sstid','weekday','weekend','starthour', 'startmonth', 'temperature','humidity','windspeed','past_trips'], outputCol='features')


    decisionTreeRegressor = DecisionTreeRegressor(featuresCol='features',labelCol='number_of_trips')
    gbtRegressor = GBTRegressor(featuresCol='features',labelCol='number_of_trips')
    randomForestRegressor = RandomForestRegressor(featuresCol='features',labelCol='number_of_trips')

    decisionTree_pipeline = Pipeline(stages=[citi_assembler, decisionTreeRegressor])
    gbt_pipeline = Pipeline(stages=[citi_assembler, gbtRegressor])
    randomForest_pipeline = Pipeline(stages=[citi_assembler, randomForestRegressor])

    decisionTree_model = decisionTree_pipeline.fit(train)
    gbt_model = gbt_pipeline.fit(train)
    randomForest_model = randomForest_pipeline.fit(train)

    decisionTree_prediction=decisionTree_model.transform(validation)
    decisionTree_prediction = decisionTree_prediction.drop('features','temperature','humidity','windspeed')
    decisionTree_prediction.write.format("org.apache.spark.sql.cassandra") \
    .options(table='dtree_weather', keyspace='bike_share_analytics').save()



    gbt_prediction = gbt_model.transform(validation)
    gbt_prediction = gbt_prediction.drop('features','temperature','humidity','windspeed')
    gbt_prediction.write.format("org.apache.spark.sql.cassandra") \
    .options(table='gbt_weather', keyspace='bike_share_analytics').save()



    randomForest_prediction = randomForest_model.transform(validation)
    randomForest_prediction = randomForest_prediction.drop('features','temperature','humidity','windspeed')
    randomForest_prediction.write.format("org.apache.spark.sql.cassandra") \
    .options(table='rf_weather', keyspace='bike_share_analytics').save()



    # evaluate the predictions
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='number_of_trips',
            metricName='r2')
    r2_decisionTree = r2_evaluator.evaluate(decisionTree_prediction)
    r2_gbt = r2_evaluator.evaluate(gbt_prediction)
    r2_randomForest = r2_evaluator.evaluate(randomForest_prediction)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='number_of_trips',
            metricName='rmse')
    rmse_decisionTree = rmse_evaluator.evaluate(decisionTree_prediction)
    rmse_gbt = rmse_evaluator.evaluate(gbt_prediction)
    rmse_randomForest = rmse_evaluator.evaluate(randomForest_prediction)

    print('Validation decision tree r2 =', r2_decisionTree)
    print('Validation gbt r2 =', r2_gbt)
    print('Validation random forest r2 =', r2_randomForest)

    print('Validation decision tree rmse =', rmse_decisionTree)
    print('Validation gbt rmse =', rmse_gbt)
    print('Validation random forest rmse =', rmse_randomForest)



if __name__ == '__main__':
    main()
