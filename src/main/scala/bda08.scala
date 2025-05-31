import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.util.Random

case class VertexData(val name: String, val likelyHood: Double, val outDegree: Int)

object bda08 {
  val timeSeries = List((0.0,3.0), (1.0,3.0), (2.0,3.0), (3.0,2.0), (4.0,1.0), (5.0,2.0), (6.0,2.0), (7.0,2.0), (8.0,3.0), (9.0,4.0))
  val edgeList = List(("A","B"), ("B","A"), ("A","C"), ("B","C"), ("C","B"))
  private val epsilon = 0.1

  def plateaus(spark: SparkSession) = {
    import org.apache.spark.mllib.rdd.RDDFunctions._

    val shuffled = spark.sparkContext.parallelize(Random.shuffle(timeSeries))
    val sorted = shuffled.sortBy(_._1)
    println("========== sorted", sorted.sliding(3).filter(window => window.map(_._2).distinct.length == 1).collect().map(_.mkString("Array(", ", ", ")")).reduce(_ + "\n" + _))
    val startOfSeq = sorted.sliding(3).filter(window => window.map(_._2).distinct.length == 1).map(it => it.head._1).collect()
    println("timestamps with 3 consecutive stable values:")
    for(start <- startOfSeq){
      println(start)
    }
  }

  def finiteDifference(spark: SparkSession) = {
    import org.apache.spark.mllib.rdd.RDDFunctions._

    val data = spark.sparkContext.parallelize(timeSeries)
    val finiteDifferences = data.sliding(2).map{window =>
      val fromTime = window(0)._1
      val toTime = window(1)._1
      val fromValue = window(0)._2
      val toValue = window(1)._2
      (toTime, (toValue-fromValue)/(toTime-fromTime))
    }.collect()
    println("slopes:")
    for(diff <- finiteDifferences){
      println(s"${diff._1}: ${diff._2}")
    }
  }

  def getGraph(spark: SparkSession) = {
    import org.apache.spark.graphx._

    val vertexIndices = edgeList.flatMap { case (a, b) => List(a, b) }.distinct.zipWithIndex.toMap
    val verticesScala = vertexIndices.map{ case(key, value) => (value.toLong, VertexData(key, 1.0/vertexIndices.size, 0))}.toSeq
    val vertices: RDD[(Long, VertexData)] = spark.sparkContext.parallelize(verticesScala)

    val edgesScala: List[Edge[Unit]] = edgeList.map{case(from, to) => Edge(vertexIndices(from), vertexIndices(to), ())}
    val edges: RDD[Edge[Unit]] = spark.sparkContext.parallelize(edgesScala)
    val graph = Graph[VertexData, Unit](vertices, edges)

    graph.joinVertices[Int](graph.outDegrees){
      case(_, data, degree) => data.copy(outDegree = degree)
    }
  }

  def printGraph(graph: Graph[VertexData, Unit]) = {
    for(v <- graph.vertices.collect().map(it => it._2)){
      println(v)
    }
  }

  def pageRankNeighbourhood(spark: SparkSession) = {
    var graph = getGraph(spark)
    val numVertices = graph.vertices.count()
    def iterate() = {
      val jumps = graph.aggregateMessages[Double](
        ctx => {
          if(ctx.srcAttr.outDegree!=0){
            ctx.sendToDst(ctx.srcAttr.likelyHood * (1 - epsilon) / ctx.srcAttr.outDegree)
          }
        },
        _+_,
      )
      val iteration = graph.vertices.cartesian(graph.vertices).map{
        case ((fromId, fromData), (toId, toData))=>(toId, fromData.likelyHood * epsilon / numVertices)
      }.union(jumps).reduceByKey(_+_)

      graph = graph.joinVertices[Double](iteration){
        case(id, data, value) => data.copy(likelyHood = value)
      }
    }
    for(i<-0 until 100){
      iterate()
    }
    iterate()
    printGraph(graph)
  }

  def pregelPageRank(spark: SparkSession) = {
    val graph = getGraph(spark)
    val n = graph.vertices.count()
    val calculated = graph.pregel(
      1.0/n,
      100,
      EdgeDirection.Out,
    )(
      (vid: VertexId, v: VertexData, d: Double)=>v.copy(likelyHood = d),
      (edge: EdgeTriplet[VertexData, Unit])=>Iterator((edge.dstId, edge.srcAttr.likelyHood / edge.srcAttr.outDegree)),
      (v1:Double, v2:Double) => v1+v2
    )
    printGraph(calculated)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PageRank").getOrCreate()
    plateaus(spark)
    finiteDifference(spark)
    //pageRankNeighbourhood(spark)
    pregelPageRank(spark)
    // testIterate(spark)
    // Thread.sleep(100000000)
    spark.stop()
  }
}
