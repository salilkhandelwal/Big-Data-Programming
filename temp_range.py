import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

def main(inputs, output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather_transformed1 = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMAX')
    weather_max = weather_transformed1.withColumn('tmax', weather_transformed1.value)
    weather_selected_max = weather_max.select("date","station","tmax")
    weather_transformed2 = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMIN')
    weather_min = weather_transformed2.withColumn('tmin', weather_transformed2.value)
    weather_selected_min = weather_min.select("date","station","tmin")
    
    weather_max_min = weather_max.join(weather_min,['station','date'],how='inner')
    weather_range = weather_max_min.withColumn('range', (weather_max_min['tmax'] - weather_max_min['tmin'])/10)

    weather_max_range = weather_range.groupby('date').max('range')
    weather_join = weather_max_range.join(weather_range,(weather_range['date']==weather_max_range['date']) \
        & ( weather_range['range']==weather_max_range['max(range)']),how='inner') \
        .drop(weather_range['date'])
    weather_final = weather_join.select('date','station','max(range)') \
        .withColumnRenamed('max(range)', 'range').sort('date','station')
    weather_final.show()
    weather_final.write.csv(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

