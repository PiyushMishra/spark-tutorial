package com.pramati.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Spike extends App {

  val filePath = "src/main/resources/README.md"

  val sparkConfig = (new SparkConf).setAppName("spike")
  sparkConfig.setMaster("local")

  val sparkContext = new SparkContext(sparkConfig)

  val data = sparkContext.textFile(filePath, 2)

  (data.foreach(a => println( a)))

  sparkContext.stop

}