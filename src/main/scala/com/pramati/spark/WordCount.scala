package com.pramati.spark

import java.sql.{ResultSet, DriverManager}

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.rdd.EsSpark

object WordCount extends App {

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "localhost").
    setAppName("WordCount").setMaster("local")

  import com.datastax.spark.connector._

  val sparkContext = new SparkContext(conf)

  val lines = sparkContext.textFile("/opt/software/spark-1.3.1-bin-hadoop2.6/README.md")

  println(lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b).collectAsMap())

  /*import org.elasticsearch.spark.rdd.EsSpark

  // define a case class
  case class Trip(departure: String, arrival: String)

  val upcomingTrip = Trip("OTP", "SFO")
  val lastWeekTrip = Trip("MUC", "OTP")
  import org.elasticsearch.spark.sql._
  val rdd = sparkContext.makeRDD(Seq(upcomingTrip, lastWeekTrip))
  EsSpark.saveToEs(rdd, "spark/docs")
  val sql = new SQLContext(sparkContext)
  val people = sql.esDF("repository/typerepository")

  people.registerTempTable("repos")
*/
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    DriverManager.getConnection("jdbc:mysql://localhost/computerDatabase?user=root");
  }
  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
  val data = new JdbcRDD(sparkContext,
    createConnection, "SELECT * FROM Companies WHERE id >= ? AND id <= ?",
    lowerBound = 1, upperBound = 4, numPartitions = 2, mapRow = extractValues)
  println(data.collect().toList)

  val cassandraTable = sparkContext.cassandraTable("system_traces" , "users")
  println(cassandraTable.getPartitions)
  // Print some basic stats on the value field.
  println(cassandraTable.map(row => row).collect().toList)

  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.client.Result
  import org.apache.hadoop.hbase.HBaseConfiguration
  val hconf = HBaseConfiguration.create()
  hconf.set(TableInputFormat.INPUT_TABLE, "tablename")
  // which table to scan
  val rdd = sparkContext.newAPIHadoopRDD(
    hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

  println(rdd.collect().toList)

  sparkContext.stop()
}
