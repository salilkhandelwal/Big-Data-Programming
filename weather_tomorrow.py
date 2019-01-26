import sys
import re
import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType())
])


def test_model(model_file):

    # load the model
    model = PipelineModel.load(model_file)
    #taken the value of the temperature from the internet 
    inputs = [
    ('Simon Fraser University',datetime.date(2018, 11, 12),49.2771, -122.9146, 330.0,12.0),
    ('Simon Fraser University',datetime.date(2018, 11, 13),49.2771, -122.9146, 330.0,12.0)  
             ]
    test = spark.createDataFrame(inputs,tmax_schema)
    pred_tom = model.transform(test)
    prediction_list = pred_tom.select('prediction').collect()
    prediction_str = str(prediction_list)
    temp_list = re.findall("\d+\.\d+", prediction_str)
    temp_str = str(temp_list)
    prediction = temp_str.replace("[","").replace("]","").replace("'","").replace("'","")
    print('Predicted tmax tomorrow:',prediction)

if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
