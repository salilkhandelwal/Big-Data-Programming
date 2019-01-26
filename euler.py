from pyspark import SparkConf, SparkContext
import sys
import random
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary



def myloop(x):
   
   
   total_iterations = 0
   random.seed()
   for i in  range(x):
     sum_total = 0.0
     while sum_total < 1:
      sum_total += random.uniform(0,1)
      total_iterations+=1
   return total_iterations



def main(inputs):
    # main logic starts here
    samples = int(inputs)
    num_slices = 4
    list_input = [int(samples/num_slices)]*num_slices
    rdd = sc.parallelize(list_input, numSlices=num_slices)
    rdd1 = rdd.map(myloop)
    print(str(rdd1.reduce(operator.add)/samples))

 

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)
