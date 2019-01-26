from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

def add_pairs(x1,x2):
    return (x1[0]+x2[0], x1[1]+x2[1])

def average(x):
    total = x[1][0]
    score = x[1][1]
    avg = total/score
    return(x[0],avg)


def relative_average(x):
    score = float(x[1][0]['score'])  
    avg = x[1][1]
    auth = x[1][0]['author']
    
    return(score/avg,auth)
    
def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    reddit_json = text.map(lambda x: json.loads(x))
    reddit_json.cache()

    subreddit_json = reddit_json.map(lambda x: (x['subreddit'],(float(x['score']),1)))
    subreddit_add = subreddit_json.reduceByKey(add_pairs)
    subreddit_average = subreddit_add.map(average)
    subreddit_average_filtered = subreddit_average.filter(lambda x : x[1] > 0)

    commentbysub = reddit_json.map(lambda c: (c['subreddit'], c))
    commenttogether = commentbysub.join(subreddit_average_filtered)
 
    final_rdd = commenttogether.map(relative_average).sortByKey(False)
    final_rdd.saveAsTextFile(output)   
  
if __name__ == '__main__':
    conf = SparkConf().setAppName('Relative Score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
