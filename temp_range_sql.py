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
    weather.createOrReplaceTempView('weather')
    weather_transformed = spark.sql('SELECT * ' \
        + 'FROM weather  WHERE qflag is null')

    weather_transformed.createOrReplaceTempView('weather_transformed')
    weather_max = spark.sql('SELECT date, station, value AS tmax ' \
        + 'FROM weather_transformed  WHERE observation="TMAX"')
    weather_min = spark.sql('SELECT date, station, value AS tmin ' \
        + 'FROM weather_transformed WHERE observation="TMIN"')


    weather_min.createOrReplaceTempView('weather_min')
    weather_max.createOrReplaceTempView('weather_max')
    weather_max_min = spark.sql('SELECT weather_max.date, weather_max.station, tmax, tmin, ' \
        + '((tmax-tmin)/10) AS range FROM weather_max inner join weather_min ' \
        + 'on weather_max.date=weather_min.date and weather_max.station=weather_min.station')
    
    weather_max_min.createOrReplaceTempView('weather_max_min')
    weather_range = spark.sql('SELECT date, max(range) AS range ' \
        + 'from weather_max_min GROUP BY date')

    weather_range.createOrReplaceTempView('weather_range')
    weather_range_fin = spark.sql('SELECT weather_range.date, station, weather_range.range ' \
        + 'FROM weather_range inner join weather_max_min ' \
        + 'on weather_range.date=weather_max_min.date and weather_range.range =weather_max_min.range ' \
        + 'ORDER BY date, station')

    weather_range_fin.createOrReplaceTempView('weather_range_fin')
    weather_range_fin.show(10)
    weather_range_fin.write.csv(output)   

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
