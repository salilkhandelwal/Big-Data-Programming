from pyspark import SparkConf, SparkContext
import sys
import math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType



def main(topic):
    spark = SparkSession.builder.appName('Streaming').getOrCreate()
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    spark.sparkContext.setLogLevel('WARN')
    values = messages.select(messages['value'].cast('string'))
    split_val = functions.split(values['value'], ' ')
    values = values.withColumn('x', split_val.getItem(0))
    values = values.withColumn('y', split_val.getItem(1))
    values = values.withColumn('xy',values['x']*values['y'])
    values = values.withColumn('x2',values['x']*values['x'])
    values.createOrReplaceTempView("updated_tbl")
    req_df = spark.sql("""Select SUM(x) as sumx, SUM(y) as sumy, SUM(xy) as sumxy,
      SUM(x2) as sumx2, COUNT(*) as n from updated_tbl""")
    slope = (req_df['sumxy']-((req_df['sumx']*req_df['sumy'])/req_df['n']))\
     /(req_df['sumx2']-(req_df['sumx']*req_df['sumx'])/req_df['n'])
    req_df = req_df.withColumn('slope',slope)
    intercept = (req_df['sumy']-req_df['slope']*req_df['sumx'])/req_df['n']
    req_df = req_df.withColumn('intercept', intercept)
    req_df.createOrReplaceTempView("final_tbl")
    print_df = spark.sql("""SELECT slope as beta,intercept as alpha FROM final_tbl""")
    stream = print_df.writeStream.format('console').outputMode('update').start()
    stream.awaitTermination(120)

if __name__ == "__main__":
    topic = sys.argv[1]
    main(topic)
