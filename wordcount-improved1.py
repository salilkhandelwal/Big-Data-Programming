from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+



# add more functions as necessary

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

def words_once(line):
    for w in wordsep.split(line.lower()):
        yield (w, 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    new_text = text.repartition(8)
    words = new_text.flatMap(words_once)
    wordsfiltered = words.filter(lambda x: '' not in x)
    wordcount = wordsfiltered.reduceByKey(operator.add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
