#entity_resolution.py
import re
import sys
import operator
from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql import functions
from pyspark.sql.functions import *
from pyspark.sql import types
from pyspark.ml.feature import * 
spark = SparkSession.builder.appName('entity resolution').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

class EntityResolution:
    def __init__(self, dataFile1, dataFile2, stopWordsFile):
        self.f = open(stopWordsFile, "r")
        self.stopWords = set(self.f.read().split("\n"))
        self.stopWordsBC = sc.broadcast(self.stopWords).value
        self.df1 = spark.read.parquet(dataFile1).cache()
        self.df2 = spark.read.parquet(dataFile2).cache()

    def preprocessDF(self, df, cols): 
        """
            Write your code!
        """ 
        column1, column2 = cols 
        df = df.withColumn('concat', 
                    functions.concat_ws(' ', functions.col(column1), functions.col(column2)))
        df = df.withColumn("concat_lower", lower(col("concat")))
        regexTokenizer = RegexTokenizer(inputCol="concat_lower", outputCol="tokens", pattern= "\W+")
        df_tokenized = regexTokenizer.transform(df)
        stopwordList = self.stopWordsBC
        remover = StopWordsRemover(inputCol="tokens", outputCol="joinKey" ,stopWords=list(stopwordList))
        df = remover.transform(df_tokenized)
        df = df.drop("concat","concat_lower","tokens")
        return df

    def filtering(self, df1, df2):
        """
            Write your code!
        """
        df1 = df1.drop("title", "description", "manufacturer", "price").cache()
        df2 = df2.drop("name", "description", "manufacturer", "price").cache()
        df1_flattened = df1.select("id", explode(col("joinKey")))
        df1_flattened = df1_flattened.selectExpr("id as id1", "col as col1")
        df2_flattened = df2.select("id", explode(col("joinKey")))
        df2_flattened = df2_flattened.selectExpr("id as id2", "col as col2")
        df1_flattened.createOrReplaceTempView('df1_flattened')
        df2_flattened.createOrReplaceTempView('df2_flattened')
        df_pair = spark.sql('SELECT df1_flattened.id1, df2_flattened.id2'\
          +' FROM df1_flattened, df2_flattened'\
          +' WHERE df1_flattened.col1 = df2_flattened.col2')
        df_pair = df_pair.dropDuplicates(['id1','id2'])
        candDF = df_pair.join(df1, df_pair.id1 == df1.id).drop("id").cache()
        candDF = candDF.withColumnRenamed("joinKey", "joinKey1")
        candDF = candDF.join(df2, candDF.id2 == df2.id).drop("id")
        candDF = candDF.withColumnRenamed("joinKey", "joinKey2")
        candDF = candDF.select("id1","joinKey1","id2","joinKey2")
        return candDF

    def verification(self, candDF, threshold):
        """
            Write your code!
        """
        @functions.udf(returnType=types.FloatType())
        def jaccard_similarity(r,s):
            R = set(r)
            S = set(s)
            RunionS = R.union(S)
            RintersectionS = R.intersection(S)
            union_size = len(RunionS)
            intersection_size = len(RintersectionS)
            similarity = float(intersection_size/union_size)
            return similarity

        js_df = candDF.withColumn('jaccard', jaccard_similarity("joinKey1", "joinKey2"))
        resultDF = js_df.filter(js_df.jaccard >= threshold)
        return(resultDF)    

    def evaluate(self, result, groundTruth):
        """
            Write your code!
        """
        try:
         r = len(result)
         a = len(groundTruth)
         t = len(set(result).intersection(set(groundTruth)))
         precision = float(t/r)
         recall = float(t/a)
         fmeasure = float((2*precision*recall)/(precision+recall))
         return(precision, recall, fmeasure)
        except ZeroDivisionError:
          print("Divide by zero error has occured, setting fmeasure to 0")
          fmeasure = 0.0
          return(precision, recall, fmeasure)      

    def jaccardJoin(self, cols1, cols2, threshold):
        newDF1 = self.preprocessDF(self.df1, cols1)
        newDF2 = self.preprocessDF(self.df2, cols2)
        print ("Before filtering: %d pairs in total" %(self.df1.count()*self.df2.count())) 

        candDF = self.filtering(newDF1, newDF2)
        print ("After Filtering: %d pairs left" %(candDF.count()))

        resultDF = self.verification(candDF, threshold)
        print ("After Verification: %d similar pairs" %(resultDF.count()))

        return resultDF

    def __del__(self):
        self.f.close()

if __name__ == "__main__":
    er = EntityResolution("Amazon_sample", "Google_sample", "stopwords.txt")
    #er = EntityResolution("Amazon", "Google", "stopwords.txt")
    amazonCols = ["title", "manufacturer"]
    googleCols = ["name", "manufacturer"]
    resultDF = er.jaccardJoin(amazonCols, googleCols, 0.5)

    result = resultDF.rdd.map(lambda row: (row.id1, row.id2)).collect()                      
    groundTruth = spark.read.parquet("Amazon_Google_perfectMapping_sample") \
                          .rdd.map(lambda row: (row.idAmazon, row.idGoogle)).collect()
    #groundTruth = spark.read.parquet("Amazon_Google_perfectMapping") \
                          #.rdd.map(lambda row: (row.idAmazon, row.idGoogle)).collect()                      
                                            
    print ("(precision, recall, fmeasure) = ", er.evaluate(result, groundTruth))