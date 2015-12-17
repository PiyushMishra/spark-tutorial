from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("intro")

sc = SparkContext(conf=conf)

array = sc.parallelize([1,2,3,4,5,6,7,8,9])

print array.collect()


