from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('tpch_2')\
 .config('spark.cassandra.connection.host', ','\
 .join(cluster_seeds))\
 .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def log_map(line):
    yield ({'orderkey':line[0], 'clerk':line[1], 'comment': line[2],\
   'custkey': line[3], 'order_priority': line[4], 'orderdate': line[5],\
   'orderstatus' : line[6],'part_names':line[9], 'ship_priority':line[7],\
   'totalprice': line[8]})

def df_table(key_space,table):
    df = spark.read.format("org.apache.spark.sql.cassandra")\
    .options(table=table, keyspace=key_space).load()
    df.registerTempTable(table)
    return df

def main(input_keyspace,output_keyspace):
    orders = df_table(input_keyspace,'orders').cache()
    line_item = df_table(input_keyspace,'lineitem')
    parts = df_table(input_keyspace,'part')
    parts_name = spark.sql("SELECT o.orderkey,o.totalprice,p.name"+\
     " FROM orders o JOIN lineitem l ON (o.orderkey = l.orderkey)"+\
     " JOIN part p ON (p.partkey = l.partkey)")
    parts_name = parts_name.groupBy(parts_name['o.orderkey'],parts_name['o.totalprice'])\
     .agg(functions.collect_set(parts_name['p.name']))
    parts_name = parts_name.rdd.map(tuple)
    order_name_temp = spark\
    .createDataFrame(parts_name, schema=['orderkey', 'total_price','part_names'])
    order_name_temp.registerTempTable('order_part')
    final = spark.sql("SELECT a.*, b.part_names FROM orders a"+\
     " JOIN order_part b ON (a.orderkey = b.orderkey)")
    final.show(5)
    final = final.rdd.flatMap(log_map).toDF()
    final.write.format("org.apache.spark.sql.cassandra")\
    .options(table='orders_parts', keyspace=output_keyspace).save()

if __name__ == "__main__":
    input_keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    main(input_keyspace,output_keyspace)

