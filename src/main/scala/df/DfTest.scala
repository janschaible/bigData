package df

import org.apache.spark.sql.SparkSession

object DfTest {

  def example(spark: SparkSession): Unit = {
    val df = spark.read.json("bigData/data/testPeople.json")
    df.createTempView("people")
    spark.sql("SELECT * FROM people").show()
  }

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("DfTest").getOrCreate()
    example(spark)
    spark.stop()
  }

}
