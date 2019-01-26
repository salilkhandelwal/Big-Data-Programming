import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('weather').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import dayofyear

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])

def main(inputs,model_file):
    data = spark.read.csv(inputs, schema=tmax_schema)
    data.createOrReplaceTempView('yesterday')
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    sqlTrans = SQLTransformer(statement="""SELECT today.station as station,today.latitude as latitude,
        today.longitude as longitude,today.elevation as elevation, dayofyear( today.date ) AS day, 
        today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday
        ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station""")
    weather_assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "day","yesterday_tmax"]\
     , outputCol="features")
    estimator = GBTRegressor(featuresCol = 'features', labelCol = 'tmax', maxIter = 50)
    pipeline = Pipeline(stages=[sqlTrans, weather_assembler, estimator])
    
    model = pipeline.fit(train)
    predictions = model.transform(validation)

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print("r-squared "+str(r2))    
    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print("root mean square error "+str(rmse))

    model.write().overwrite().save(model_file)
   


if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs,model_file)

