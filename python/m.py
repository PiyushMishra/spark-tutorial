from pyspark import SparkConf, SparkContext

conf=SparkConf().setAppName("myPythonApp").setMaster("local")
sc=SparkContext(conf=conf)

list=sc.parallelize([1,2,3])
print list.first()
print list.count()
print list.reduce(lambda x,y : x +y)
lines=sc.textFile("src/main/resources/README.md")
print lines.flatMap(lambda x :  x.split(" ")).map(lambda x : (x,1)).reduceByKey(lambda x,y :  x+y).collectAsMap()


