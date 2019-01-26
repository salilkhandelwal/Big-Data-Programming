from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql import SQLContext, Row
import sys
import re
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

line_re = r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$'

schema = types.StructType([
    types.StructField('hostname', types.StringType(), False),
    types.StructField('bytes', types.FloatType(), False),
   ])

# add more functions as necessary

def match_fn(line):
    line_match = re.match(line_re, line)
    if line_match is not None:
      yield (line_match.groups(0))

def main(inputs):
    # main logic starts here
    text = sc.textFile(inputs)
    nasa_rdd = text.flatMap(match_fn).map(lambda x: (x[0],float(x[3])))
    nasa_df = sqlContext.createDataFrame(nasa_rdd,schema)
    nasa_xy = nasa_df.groupBy(nasa_df.hostname).agg(functions.count('*')\
      .alias('x'),functions.sum(nasa_df.bytes).alias('y')).drop('hostname')
    nasa_calc = nasa_xy.withColumn('n',functions.lit(1))\
      .withColumn('x2',nasa_xy.x * nasa_xy.x)\
      .withColumn('y2',nasa_xy.y * nasa_xy.y).withColumn('xy',nasa_xy.y * nasa_xy.x)
    nasa_sum = nasa_calc.groupBy().sum()
    nasa_sum.show()
    nasa_correlation = nasa_sum\
      .withColumn('r',(nasa_sum['sum(n)'] * nasa_sum['sum(xy)'] - nasa_sum['sum(x)']\
      * nasa_sum['sum(y)']) / (functions.sqrt(nasa_sum['sum(n)'] * nasa_sum['sum(x2)']\
      - nasa_sum['sum(x)']**2) * functions.sqrt(nasa_sum['sum(n)'] * nasa_sum['sum(y2)']\
      - nasa_sum['sum(y)']**2)))
    nasa_final = nasa_correlation.withColumn('r2',nasa_correlation.r**2)
    to_list = [list(row) for row in nasa_final.collect()]
    print("r = "+str(to_list[0][6]))
    print("r^2 = "+str( to_list[0][7]))
    
    
if __name__ == '__main__':
    conf = SparkConf().setAppName('Nasa-Logs')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)                                                      
