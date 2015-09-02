package com.pramati.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Spike extends App {

  val filePath = "src/main/resources/README.md"

  val sparkConfig = (new SparkConf).setAppName("spike")
  sparkConfig.setMaster("local")

  val sc = new SparkContext(sparkConfig)

  val lines = sc.textFile(filePath, 2)

  (lines.foreach(a => println(a)))

  val tuples = sc.parallelize(List(("coffie", 1), ("apple", 1), ("guava", 3), ("coffie", 8)))
  
  val tuples2 = sc.parallelize(List(("coffie", 6), ("apple", 3), ("guava", 3), ("tea", 9)))
  
  
  val finalRDD = tuples.join(tuples2)

  val result = tuples.combineByKey(
    (v) => (v, 1),
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).
    map { case (key, value) => (key, value._1 / value._2.toFloat) }

  result.collectAsMap().map(println(_))

  val input = sc.parallelize(List(1,2,3,4,5,6,7))

  val result1 = input.aggregate((0, 0))(
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
  )

  println(lines.flatMap(a => a.split(" ")).map(a => (a,1)).reduceByKey((a ,b) => a+ b).collectAsMap())

  
  sc.stop
  
  val customerkey = "X3LoqzZzGOxrdWHWzwIDTRCtw"
  val customerSecret = "Knm8W31dOSsxDwuMRxdoWvOrQHyTcMYvn0ASS5AEagv1ObMTDO"
  val token = "574247165-7fWd1iyEfYS3m7XBz9gZ62Tyogydofaf5QWDOpdl"
  val tokenSecret = "AC8SAU8ttkRREWp04rtbR9IIgOK7SysADLq3GJRVywiOI"

} 