import sbt.Keys._

name:="spark-tutorial"

version:="1.0"

scalaVersion:="2.11.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1"

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies +="org.elasticsearch" % "elasticsearch-spark_2.11" % "2.1.0.Beta4"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.1.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.37"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M2"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.94.16"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.4.1"



