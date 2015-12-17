package com.pramati.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by piyushm on 17/12/15.
 */
object GraphXApp extends App {

  val sparkConf = new SparkConf().setAppName("graphxApp").setMaster("local[*]")

  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  val sc = new SparkContext(sparkConf)

    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

  class VertexProperty

  case class UserProperty(val name: String) extends VertexProperty

  case class ProductProperty(val name: String, val price: Double)
    extends VertexProperty

  val defaultUser = ("John Doe", "Missing")
  val graph = Graph(users, relationships, defaultUser)

  println(graph.vertices.filter {
    case (vertexId, (name, value)) => value == "student"
  }.collect().toList
  )

  println(graph.edges.filter(edge => edge.dstId > edge.srcId).count())


  println(graph.triplets.map(triplet => triplet.srcAttr._1 + " " +
    "is the " + triplet.attr + " of " + triplet.dstAttr._1).collect().toList)

  println(graph.inDegrees)
}
