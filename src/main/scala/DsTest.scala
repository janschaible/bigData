import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

case class Person(name: String, age: Int)

object DsTest {

  def dsTest(spark: SparkSession): Unit = {
    import spark.implicits._

    val ds = spark.read.json("bigData/data/person.json").withColumn("age", col("age").cast("int")).as[Person]
    val adults = ds.filter(_.age >= 18)
    adults.show()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DsTest").getOrCreate()
    dsTest(spark)
    spark.close()
  }
}
