from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")

sc = SparkContext(conf = conf)

lines = sc.textFile("/opt/software/spark-1.3.1-bin-hadoop2.6/README.md")

print lines.flatMap(lambda line : line.split(" ")).map(lambda word : (word, 1)).reduceByKey(lambda a,b : a + b).collect()
