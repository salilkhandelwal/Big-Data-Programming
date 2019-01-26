import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.context import SQLContext

spark = SparkSession.builder.appName('example application').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+


def getedges(line):
	list1 = line.split(':')
	if list1[0] == '':
		return None
	else:
		s = list1[0]
	list2 = list1[1].split(' ')
	list2 = filter(None, list2)
	results = []
	for d in list2:
		results.append((s, d))
	return (results)

def get_source(val, resulted_df, l_output):
	result_row = resulted_df.where(resulted_df.node == val).select("source")
	result = result_row.rdd.flatMap(list).first()
	l_output.append(result)
	if(result != source_node):
		return(get_source(result, resulted_df, l_output))
	else:
		return(l_output)

# main logic
def main(inputs,output,source_node,dest_node):
	if(source_node == dest_node):
		print('source is same as destination')
		sys.exit()
	textinput = sc.textFile(inputs + '/links-simple-sorted.txt')
	graphedges_rdd = textinput.map(lambda line: getedges(line))\
	        .filter(lambda x: x is not None).flatMap(lambda x: x)
	graphedges = graphedges_rdd.toDF(['source', 'destination']).cache()
	print('\nconstructed edge graph df from given input')
	KnownRow = Row('node', 'source', 'distance')
	schema = StructType([
		StructField('node', StringType(), False),
		StructField('source', StringType(), False),
		StructField('distance', IntegerType(), False),
		])

	newRow = KnownRow(source_node,-1, 0)
	finalOut = sqlContext.createDataFrame([newRow], schema=schema).cache()

	inter_df = finalOut
	for i in range(6):
		print('loop: ',i)
                # There are not many elements so coalesce is performed
		finalOut.coalesce(1).write.json(output + '/iter' + str(i))
		if (inter_df.filter(inter_df.node == dest_node).count() > 0):
			print('match found')
			break
		elif (i == 6):
			print('no match within 6 steps')
			finalOut = sqlContext.createDataFrame(sc.emptyRDD(), schema)
			break
		cond = [inter_df['node'] == graphedges['source']]
		df_result = graphedges.join(inter_df, cond, 'inner')\
		       .select(graphedges['destination'].alias('node'), graphedges['source'],\
		       (inter_df['distance'] +1).alias('distance')).cache()		
		if (df_result.rdd.isEmpty()):
			print('no match found')
			finalOut = sqlContext.createDataFrame(sc.emptyRDD(), schema)
			break
		df_result = df_result.join(finalOut, ['node'], "leftanti").cache()
		inter_df = df_result
		finalOut = finalOut.union(df_result)

	if len (finalOut.take(1)) != 0 :
		l = get_source(dest_node, finalOut, [dest_node])
		l.reverse()
		print(l)
		
	print_df = spark.createDataFrame(l,StringType())
        # There are not many elemetns so coalesce is performed
	print_df.coalesce(1).write.text(output +'/path')

if __name__ == "__main__":
	 
         inputs = sys.argv[1]
         output = sys.argv[2]
         source_node = sys.argv[3]
         dest_node = sys.argv[4]
         main(inputs,output,source_node,dest_node)
