import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import lit,sqrt
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example')\
 .config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
sc = spark.sparkContext

def main(table,keyspace):
    Corelationdf = spark.read.format("org.apache.spark.sql.cassandra")\
     .options(table=table, keyspace=keyspace).load()
    nasa_df = Corelationdf.groupby('Host')\
     .agg({'Host':'count','bytes':'sum'})\
     .withColumnRenamed('count(Host)','X')\
     .withColumnRenamed('sum(bytes)','Y')
    nasa_calc_df = nasa_df.select(nasa_df['Y'],nasa_df['X'],(nasa_df['Y']**2)\
     .alias('Y2'),(nasa_df['X']**2)\
     .alias('X2'),(nasa_df['X']*nasa_df['Y']).alias('XY'))
    nasa_fin_df = nasa_calc_df.withColumn('N',lit(1))
    nasa_sum_df = nasa_fin_df.agg({'X':'sum','Y':'sum','X2':'sum','Y2':'sum','XY':'sum','N':'sum'})
    nasa_rename_df = nasa_sum_df.withColumnRenamed('sum(X2)','X2')\
     .withColumnRenamed('sum(Y2)','Y2').withColumnRenamed('sum(X)','X')
    nasa_corr_df = nasa_rename_df.withColumnRenamed('sum(N)','N').withColumnRenamed('sum(Y)','Y')\
     .withColumnRenamed('sum(XY)','XY')
    R = nasa_corr_df.select((nasa_corr_df['N']*nasa_corr_df['XY']-nasa_corr_df['X']*nasa_corr_df['Y'])\
     .alias('UP'),(sqrt(nasa_corr_df['N']*nasa_corr_df['X2']- nasa_corr_df['X']**2)\
     * sqrt(nasa_corr_df['N']*nasa_corr_df['Y2'] - nasa_corr_df['Y']**2)).alias('DOWN'))
    Out = R.select((R['UP']/R['DOWN']).alias('r'),((R['UP']/R['DOWN'])**2).alias('r2')).collect()
    print('R =',Out[0][0])
    print('R2 =',Out[0][1])


if __name__ == '__main__':
    keyspace = sys.argv[1]
    table = sys.argv[2]
    main(table,keyspace)

