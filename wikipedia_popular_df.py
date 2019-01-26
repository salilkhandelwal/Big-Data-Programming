import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    hour = re.split("/",path).pop().replace('pagecounts-','').replace('.gz','')\
      .replace('0000','')
    return hour
   

def main(inputs, output):
    # main logic starts here
    
    observation_schema = types.StructType([
    types.StructField('language', types.StringType(), False),
    types.StructField('title', types.StringType(), False),
    types.StructField('page_count', types.IntegerType(), False),
    types.StructField('bytes', types.IntegerType(), False),
  ])
    wiki_data = spark.read.csv(inputs, sep=' ', schema=observation_schema).withColumn('hour',\
     path_to_hour(functions.input_file_name()))
    wiki_data_filter = wiki_data\
    .filter((wiki_data.language == 'en') & ~(wiki_data.title == 'Main_Page') & ~(wiki_data.title.startswith('Special:')))
    wiki_data_filter.cache() 
    
    wiki_max = wiki_data_filter.select('hour','page_count').groupBy('hour').max('page_count')\
      .orderBy(wiki_data_filter.hour.asc())
    wiki_max_rename = wiki_max.select(functions.col("hour").alias("hours"), functions.col("max(page_count)")\
      .alias("views"))
    cond = [wiki_data_filter.page_count == wiki_max_rename.views, wiki_data_filter.hour == wiki_max_rename.hours]
    wiki_join = wiki_data_filter.join(functions.broadcast(wiki_max_rename), cond, 'inner')
    wiki_projection = wiki_join.select('hour','title','views')
    wiki_projected_sort = wiki_projection.orderBy(wiki_projection.hour.asc(), wiki_projection.title.asc())
    wiki_projected_sort.explain()
    # coalesce is used as the question requires an output in a newline-delimited JSON
    wiki_projected_sort.coalesce(1).write.json(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

