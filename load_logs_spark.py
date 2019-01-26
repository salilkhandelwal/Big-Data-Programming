from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def match_fn(line):
    line_match = re.match(linex, line)
    if line_match is not None:
      yield ({'uid':str(uuid.uuid1()), 'host':line_match.group(1), \
      'datetime': datetime.datetime.strptime(line_match.group(2), '%d/%b/%Y:%H:%M:%S'),\
      'path': line_match.group(3),'bytes': float(line_match.group(4))})

def main(inputs,key_space,table):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('Load Spark')\
     .config('spark.cassandra.connection.host', ','\
     .join(cluster_seeds)).getOrCreate()
    sc = spark.sparkContext
    text = sc.textFile(inputs)
    partition_count = 120;
    text = text.repartition(partition_count)
    result = text.flatMap(match_fn).toDF()
    result.write.format("org.apache.spark.sql.cassandra")\
     .options(table=table, keyspace=key_space).save()

if __name__ == "__main__":
    inputs = sys.argv[1]
    key_space = sys.argv[2]
    table = sys.argv[3]
    reg = "^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$"
    linex = re.compile(reg)
    main(inputs,key_space,table)
                             
