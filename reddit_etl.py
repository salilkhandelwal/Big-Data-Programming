from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def transform(line):
    return (line['subreddit'], float(line['score']), line['author'])



def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    transformed_data = text.map(lambda x: json.loads(x)).map(transform).filter(lambda x: 'e' in x[0])
    transformed_data.cache() 
    transformed_data_pos = transformed_data.filter(lambda x: x[1] > 0)
    transformed_data_neg = transformed_data.filter(lambda x: x[1] <= 0)
    transformed_data_pos.map(json.dumps).saveAsTextFile(output + '/positive')
    transformed_data_neg.map(json.dumps).saveAsTextFile(output + '/negative')
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
