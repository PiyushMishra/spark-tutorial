package com.pramati.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by piyushm on 17/12/15.
 */
object GraphXApp extends App {

  val sparkConf = new SparkConf().setAppName("graphxApp").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD

  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof")), (4L, ("peter", "student"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"), Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))

  val defaultUser = ("John Doe", "Missing")
  val graph = Graph(users, relationships, defaultUser)

  println(graph.vertices.filter { case (vertexId, (name, value)) => value == "student"
  }.collect().toList)

  println(graph.edges.filter(edge => edge.dstId > edge.srcId).count())

  println(graph.triplets.map(triplet => triplet.srcAttr._1 + " " +
    "is the " + triplet.attr + " of " + triplet.dstAttr._1).collect().toList)

  println("###########################################################################################")

  val subGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "student")

  println(subGraph.triplets.map(triplet => triplet.srcAttr._1 + " " +
    "is the " + triplet.attr + " of " + triplet.dstAttr._1).collect().toList)

  import org.apache.spark.graphx.util.GraphGenerators

  val randomGraph = GraphGenerators.logNormalGraph(sc, 100).mapVertices((id, _) => id.toDouble)


  val olderFollowers = randomGraph.aggregateMessages[(Int,Double)](triplet => if(triplet.srcAttr > triplet.dstAttr) {
  triplet.sendToDst((1,triplet.srcAttr))
  },
  (a, b) => (a._1 + b._1, a._2 + b._2)
)

  val avgAgeOfOlderFollowers: VertexRDD[Double] =
    olderFollowers.mapValues((id, value) => value match {
      case (count, totalAge) => totalAge / count
    })

  avgAgeOfOlderFollowers.collect foreach(println _)
  println("############################")
  println("############################" +
    graph.collectNeighborIds(EdgeDirection.Out).collect.size)


  def max(a:(VertexId,Int), b:(VertexId,Int)) = if(a._2 > b._2) a else b

  println(graph.inDegrees.reduce(max))
  println(graph.outDegrees.reduce(max))
  println(graph.degrees.reduce(max))


}
