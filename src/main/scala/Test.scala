import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


class Test {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Test-Local")
    .getOrCreate

  val test = spark.read.format("csv")
    .options(Map("header" -> "true", "delimiter" -> ","))
    .load("/home/spineor/Desktop/test")

  test.printSchema()

  test.show(false)

  spark.time(test.count())

  test

}

object Test {
  def apply(): Test = new Test()
}