package com.pramati.spark.mllib

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by piyushm on 7/10/15.
 */
object MLlibApp extends App {


  val conf = new SparkConf().setMaster("local").setAppName("mllib")
  val sc = new SparkContext(conf)

  val spam: RDD[String] = sc.textFile("src/main/resources/spam.txt")
  val normal: RDD[String] = sc.textFile("src/main/resources/normal.txt")

  val tf: HashingTF = new HashingTF(10000)

  val spamFeatures: RDD[Vector] = spam.map{email =>
    tf.transform(email.split(" "))}

  val normalFeatures: RDD[Vector] = normal.map(email => tf.transform(email.split(" ")))

  val positiveExamples: RDD[LabeledPoint] = spamFeatures.map(features => LabeledPoint(1.0, features))

  val negativeExamples: RDD[LabeledPoint] = normalFeatures.map(features => LabeledPoint(0.0, features))

  val trainingData: RDD[LabeledPoint] = positiveExamples.union(negativeExamples)

  println(trainingData.collect())

  trainingData.cache()

  // Run Logistic Regression using the SGD algorithm.
  val model: LogisticRegressionModel = new LogisticRegressionWithSGD().run(trainingData)

  // Test on a positive example (spam) and a negative one (normal).
  val posTest: Vector = tf.transform(("YOUR COMPUTER HAS BEEN INFECTED!  YOU MUST RESET YOUR PASSWORD.  " +
    "Reply to this email with your password and SSN ...").split(" "))
  val negTest: Vector = tf.transform(("Send money and get access to awesome stuff").split(" "))
  println("Prediction for positive test example: " + model.predict(posTest))
  println("Prediction for negative test example: " + model.predict(negTest))

  sc.stop()
}
