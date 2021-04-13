import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object SparkDemoStructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String]
//        .flatMap(x => x.split(" "))
          .flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "5 seconds", "5 seconds"),
        $"value")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}