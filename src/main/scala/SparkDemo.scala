import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkDemo {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "SparkDemo")

    val lines = sc.parallelize(List("hello world", "hello spark", "hello", "sevak", "avetisyan", "spark"))
    val wordCountRdd = lines
      .flatMap(line => line.split(" "))
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .cache()

    wordCountRdd.foreach(println)

    wordCountRdd
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
      .take(3)
      .foreach(println)
  }
}