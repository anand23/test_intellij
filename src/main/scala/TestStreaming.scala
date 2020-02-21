import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

class TestStreaming extends java.io.Serializable {

  val conf:SparkConf = new SparkConf().setAppName("TestStream").setMaster("local[*]")
  val sc:SparkContext = new SparkContext(conf)

  sc.setLogLevel("ERROR")

  val ssc = new StreamingContext(sc, Seconds(5))

  case class Emp(id: Int, name: String, sal: Double)

  val empDtream = ssc.textFileStream("/home/spineor/Desktop/Stream/")

//  empDtream.foreachRDD(rdd => rdd.map(line => line.split(",")).map(c => Emp(c(0).toInt, c(1), c(2).toDouble)).foreach(println))

  println("Nb lines is equal to = " + empDtream.count())
  empDtream.foreachRDD { (rdd, time) => println(rdd.count()) }

  ssc.start()

  ssc.awaitTermination()

}

object TestStreaming {
  def apply(): TestStreaming = new TestStreaming()
}