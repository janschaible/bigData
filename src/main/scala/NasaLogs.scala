import org.apache.spark.sql.SparkSession

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter


// unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985

/**
 * +-----+------+
 * |hours| count|
 * +-----+------+
 * |   -1|     1|
 * |    0| 62450|
 * |    1| 53066|
 * |    2| 45297|
 * |    3| 37398|
 * |    4| 32234|
 * |    5| 31919|
 * |    6| 35253|
 * |    7| 54017|
 * |    8| 83750|
 * |    9| 99969|
 * |   10|105507|
 * |   11|115720|
 * |   12|122085|
 * |   13|120814|
 * |   14|122479|
 * |   15|121200|
 * |   16|118037|
 * |   17| 97609|
 * |   18| 79282|
 * |   19| 71776|
 * |   20| 69809|
 * |   21| 71922|
 * |   22| 70759|
 * |   23| 69362|
 * +-----+------+
 */

/**
 * +--------------------+----------+-----+
 * |                 url|statusCode|count|
 * +--------------------+----------+-----+
 * |pub/winvn/readme.txt|       404|  667|
 * |pub/winvn/release...|       404|  547|
 * |history/apollo/ap...|       404|  286|
 * |history/apollo/a-...|       404|  230|
 * |shuttle/resources...|       404|  230|
 * |history/apollo/pa...|       404|  215|
 * |://spacelink.msfc...|       404|  215|
 * |images/crawlerway...|       404|  214|
 * |history/apollo/sa...|       404|  183|
 * |shuttle/resources...|       404|  180|
 * |shuttle/missions/...|       404|  175|
 * |shuttle/missions/...|       404|  168|
 * |elv/DELTA/uncons.htm|       404|  163|
 * |history/apollo/pu...|       404|  140|
 * |shuttle/missions/...|       404|  107|
 * |shuttle/resources...|       404|   92|
 * |procurement/procu...|       404|   86|
 * |history/apollo-13...|       404|   73|
 * |history/apollo/pa...|       404|   71|
 * |shuttle/countdown...|       404|   68|
 * +--------------------+----------+-----+
 */

object NasaLogs {

  val textFile = "bigData/data/NASA_access_log_Jul95"
  val urlCodePattern = """^.*".* /(.*) HTTP.*" ([0-9]+) .*$""".r

  def countVisitsPerHour(): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    val logData = spark.read.text(textFile)
    logData
      .map(it => try{
        ZonedDateTime.parse(it.getString(0).split('[')(1).split(']')(0), DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")).getHour
      } catch {
        case e: Exception => -1
      })
      .groupBy("value")
      .count()
      .sort("value")
      .show(25)

    spark.stop()
  }

  def statusCodesByUrl(): Unit = {
    val spark = SparkSession.builder().appName("StatusCodesByUrl").getOrCreate()
    import spark.implicits._

    spark.read.textFile(textFile)
      .map {
        case urlCodePattern(url, code) => (url, code.toInt)
        case _ => null
      }.filter(it => it !=null && it._2 == 404)
      .toDF("url", "statusCode")
      .groupBy("url", "statusCode")
      .count()
      .sort($"count".desc)
      .show(20)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    statusCodesByUrl()
  }
}
