package com.pramati.spark

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.Logging
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.StorageLevels
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization

object TwitterStream extends App {

  val customerKey = "X3LoqzZzGOxrdWHWzwIDTRCtw"
  val customerSecret = "Knm8W31dOSsxDwuMRxdoWvOrQHyTcMYvn0ASS5AEagv1ObMTDO"
  val appToken = "574247165-7fWd1iyEfYS3m7XBz9gZ62Tyogydofaf5QWDOpdl"
  val appsecret = "AC8SAU8ttkRREWp04rtbR9IIgOK7SysADLq3GJRVywiOI"

  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]").setAppName("streaming")
  val streamingContext = new StreamingContext(sparkConf, Seconds(1))

  val configurationBuilder = new ConfigurationBuilder()
  configurationBuilder.setOAuthConsumerKey(customerKey)
  configurationBuilder.setOAuthConsumerSecret(customerSecret)
  configurationBuilder.setOAuthAccessToken(appToken)
  streamingContext.checkpoint("~/")
  configurationBuilder.setOAuthAccessTokenSecret(appsecret)

  val factory = new TwitterFactory(configurationBuilder.build())

  val twitterStream = TwitterUtils.createStream(streamingContext, Some(factory.getInstance.getAuthorization), Array("software"),
    StorageLevels.MEMORY_AND_DISK)

  val statuss = twitterStream flatMap { x => x.getText.split(" ") }
  val hashtags = statuss.filter(_.startsWith("#"))
  hashtags.map(tag => (tag, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 5), Seconds(1)).print()
  streamingContext.start
  streamingContext.awaitTermination()

}