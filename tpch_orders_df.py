from pyspark import SparkConf, SparkContext
import sys
import math
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('tpch_1')\
.config('spark.cassandra.connection.host', ','.join(cluster_seeds))\
.config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(x):
        orderkey, price, names = x
        namestr = ', '.join(sorted(list(names)))
        return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def read_table(key_space,table):
        df = spark.read.format("org.apache.spark.sql.cassandra")\
         .options(table=table, keyspace=key_space).load()
        return df

def get_order_keys(order_keys):
        sql = '('
        for i in order_keys:
            sql = sql + str(i) + ','
        sql = sql[:-1] + ')'
        return sql

def main(key_space,out_dir,order_keys):
        keys = get_order_keys(order_keys)
        orders = read_table(key_space,'orders')
        orders.registerTempTable('orders')
        line_item = read_table(key_space,'lineitem')
        line_item.registerTempTable('lineitem')
        part = read_table(key_space,'part')
        part.registerTempTable('part')
        result = spark\
        .sql("SELECT o.orderkey,o.totalprice,p.name FROM orders o JOIN"+\
        " lineitem l ON (o.orderkey = l.orderkey)  JOIN part p ON (p.partkey =  l.partkey)")\
        .where('orderkey in' + keys)
        result = result.groupBy(result['o.orderkey'],result['o.totalprice'])\
        .agg(functions.collect_set(result['p.name']))
        result.explain()
        output = result.rdd.map(tuple).sortBy(lambda x: x[0])
        output = output.map(output_line)
        #It is small enough to coalesce
        output.coalesce(1).saveAsTextFile(out_dir)

#orderkey, price, names  
if __name__ == "__main__":
        key_space = sys.argv[1]
        out_dir = sys.argv[2]
        order_keys = sys.argv[3:]
        main(key_space,out_dir,order_keys)

