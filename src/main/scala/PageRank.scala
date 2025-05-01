import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// A=0.23, B=0.43, C=0.33
// graphx implementiert den algorithmus schon..

object PageRank {
  private val epsilon = 0.1

  private def addSelfLoops(g: RDD[(String, String)], nodes: RDD[String]): RDD[(String, String)] = {
    val sinks = nodes.subtract(g.keys)
    val selfLoops: RDD[(String, String)] = sinks.map(it => (it, it))

    g.union(selfLoops)
  }

  private def numOfOutgoingNodes(g: RDD[(String, String)]): RDD[(String, Int)] = {
    g.mapValues(it =>  1)
      .reduceByKey((a,b)=> a+b)
  }

  private def checkProbabilities(g: RDD[(String, (String, Double))]): Unit = {
    val probabilities = g.map(it => (it._1, it._2._2))
      .reduceByKey((a,b) => a + b)
    println("========== probabilities", probabilities.collect().mkString("Array(", ", ", ")"))
  }

  private def checkProbabilitiesDistribution(g: RDD[(String, Double)]): Unit = {
    val probability = g.map(it => it._2).sum()
    println("========== probabilities", probability)
  }

  private def iterate(distribution: RDD[(String, Double)], transitionProbabilities: RDD[(String, (String, Double))]): RDD[(String, Double)] = {
    distribution.join(transitionProbabilities)
      .map(it => (it._2._2._1, it._2._1 * it._2._2._2))
      .reduceByKey((a,b)=>a+b)
  }

  private def iterateK(distribution: RDD[(String, Double)], transitionProbabilities: RDD[(String, (String, Double))], k: Int): RDD[(String, Double)] = {
    var res = distribution
    for(i <- 1 to k){
      res = iterate(res, transitionProbabilities)
    }
    res
  }

  private def pageRank(spark: SparkSession): Unit = {
    val g = spark.sparkContext.parallelize(List.apply(("A","B"), ("B","A"), ("A","C"), ("B","C"), ("C","B")))
    val nodes = g.keys.union(g.values).distinct()
    val n = nodes.count()

    val gWithSelfLoops = addSelfLoops(g, nodes)
    val gr = nodes.cartesian(nodes).map{case(from, to)=>(from, (to, epsilon/n))}

    val outCount = numOfOutgoingNodes(gWithSelfLoops)
    val gp = gWithSelfLoops.join(outCount).map{case(from, (to, count)) => (from, (to, (1.0/count)-(epsilon/count)))}


    val gf = gr.union(gp)
    checkProbabilities(gf)

    val start = nodes.map(it => (it, 1.0/n))
    val res = iterateK(start, gf, 100)
    // checkProbabilitiesDistribution(res)
    // println("========== result", res.collect().mkString("Array(", ", ", ")"))

    val start2 = spark.sparkContext.parallelize(List.apply(("A", 1.0),("B", 0.0),("C", 0.0)))
    val res2 = iterateK(start2, gf, 100)
    println("========== result", res2.collect().mkString("Array(", ", ", ")"))
    // Converge to the same numbers (A,0.22988505747126467), (B,0.4367816091954029), (C,0.33333333333333376))
  }

  private def testIterate(spark: SparkSession) = {
    val dist = spark.sparkContext.parallelize(List.apply(("a", 1.0), ("b", 0.0), ("c", 0.0)))
    val prob = spark.sparkContext.parallelize(List.apply(("a", ("a", 0.1)), ("a", ("a", 0.1)), ("a", ("b", 0.3)), ("a", ("c", 0.5))))

    val res = iterate(dist, prob)
    println(res.collect().mkString("Array(", ", ", ")"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PageRank").getOrCreate()
    pageRank(spark)
    // testIterate(spark)
    spark.stop()
  }
}
