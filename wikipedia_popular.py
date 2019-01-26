from pyspark import SparkConf, SparkContext
import sys
import operator

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    thisList = line.split()
    timestamp = thisList[0]
    lang = thisList[1]
    title = thisList[2]
    count = int(thisList[3])
    byte = thisList[4]
    tup = (timestamp, lang, title, count, byte) 
    return tup

def rem(t):
    if(t[1].startswith("en")):
        if((t[2].lower() != "main_page") and (not t[2].startswith("Special:"))):
            return t

def get_kv(kv):
    tup = (kv[3],kv[2])
    return (kv[0],tup)

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])
               
      
    

text = sc.textFile(inputs)
tupform = text.map(words_once)
tupform_filtered = tupform.filter(rem)
rdd = tupform_filtered.map(get_kv)
max_count = rdd.reduceByKey(max)

outdata = max_count.sortBy(get_key).map(tab_separated).saveAsTextFile(output)

