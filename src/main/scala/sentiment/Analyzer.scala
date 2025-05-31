package sentiment

import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.linalg.Vectors.sparse
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class Analyzer {
  val spark = SparkSession.builder().appName("Sentiment analysis").getOrCreate()

  def getTweets(): Dataset[Tweet] = {
    import spark.implicits._

    spark.read
      .option("header", "false")
      .csv("bigData/data/tweets.csv")
      .toDF("target", "id", "date", "flag", "user", "text")
      .withColumn("target", col("target").cast("int"))
      .as[Tweet]
  }

  def getStopWords(): Set[String] = {
    val source = Source.fromFile("bigData/data/stopwords.txt")
    source.getLines().toSet
  }

  def getWords(tweets: Dataset[Tweet]): Dataset[Array[String]] = {
    import spark.implicits._
    val stopWords = spark.sparkContext.broadcast(getStopWords())

    tweets.map(
      tweet => tweet.text.split("\\s+")
        .map(word => word.toLowerCase())
        .filter(word => !stopWords.value.contains(word) && word.matches("^\\d+$|^[a-zA-Z]+$"))
    )
  }

  def getDocTermFreq(wordsDs: Dataset[Array[String]]): Dataset[Map[String, Int]] = {
    import spark.implicits._
    wordsDs.map(words => words.groupBy(identity).mapValues(_.length))
  }

  def getDocFreqs(docTermFreqs: Dataset[Map[String, Int]]): RDD[(String, Int)] = {
    docTermFreqs.rdd
      .flatMap(docTermFreq => docTermFreq.keys.map(key => (key, 1)))
      .reduceByKey(_ + _)
  }

  def topTermsInTopConcepts(
                             svd: SingularValueDecomposition[RowMatrix, Matrix],
                             numConcepts: Int,
                             numTerms: Int, termIdMap: Map[String, Int])
  : Seq[Seq[(String, Double)]] = {
    val termIds = termIdMap.toSeq.map{case(key, value) => (value, key)}.toMap
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_
        ._1)
      topTerms += sorted.take(numTerms).map {
        case (score, id) => (termIds(id), score)
      }
    }
    topTerms
  }

  def topDocsInTopConcepts(
                            svd: SingularValueDecomposition[RowMatrix, Matrix],
                            numConcepts: Int, numDocs: Int)
  : Seq[Seq[(Long, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(Long, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_
        .toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map {
        case (score, id) => (id, score)
      }
    }
    topDocs
  }

  def analyze() = {
    val tweets = getTweets()
    val words = getWords(tweets)
    val docTermFreq = getDocTermFreq(words)
    val docFreqs = getDocFreqs(docTermFreq)
    val topDocFreqs = docFreqs.takeOrdered(2000)(Ordering.by(_._2)).toSeq
    val n = docTermFreq.count()
    val idfs: Map[String, Double] = topDocFreqs.map { case (term, freq) =>
      term -> math.log(n / freq)
    }.toMap
    val idTerms: Map[String, Int] = idfs.keys.zipWithIndex.toMap

    val bIdfs = spark.sparkContext.broadcast(idfs)
    val bIdTerms = spark.sparkContext.broadcast(idTerms)

    val vecs: RDD[Vector] = docTermFreq.rdd.map(termFreqs => {
      val termsInDoc = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bIdTerms.value.contains(term)
      }.map {
        case (term, freq) => (bIdTerms.value(term), bIdfs.value(term) * termFreqs(term) / termsInDoc)
      }.toSeq
      sparse(bIdTerms.value.size, termScores)
    })

    vecs.cache()
    val numConcepts = 100
    val result = new RowMatrix(vecs).computeSVD(numConcepts, computeU = true)

    val topConceptTerms = topTermsInTopConcepts(result, 4, words.count().toInt, idTerms)
    val topConceptDocs = topDocsInTopConcepts(result, 4, words.count().toInt)

    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }
    Thread.sleep(1000000)
  }

  def stop() = {
    spark.stop()
  }
}
