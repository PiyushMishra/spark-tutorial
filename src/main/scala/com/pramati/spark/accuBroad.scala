package com.pramati.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by piyushm on 29/11/15.
 */
object accuBroad extends App {
  val conf = new SparkConf().setAppName("accuBroad").setMaster("local")
  val sc = new SparkContext(conf)
  val file = sc.textFile("src/main/resources/callsigns")

  val ac = sc.accumulator(0)


  val callSigns = file.flatMap { line =>
    if (line == "") ac += 1
    line.split(" ")
  }

  println(callSigns.collect().toList)
  println(ac.value)


}
