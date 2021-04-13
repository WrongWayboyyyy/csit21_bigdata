import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkDemoStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    import org.apache.spark._

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc
      .socketTextStream("localhost", 9999)

    lines
      .flatMap(x => x.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .print()

    //    lines.flatMap(line => line.split(" "))
    //      .foreachRDD { rdd =>
    //        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    //        import spark.implicits._
    //
    //        // Convert RDD[String] to DataFrame
    //        val wordsDataFrame = rdd.toDF("word")
    //
    //        // Create a temporary view
    //        wordsDataFrame.createOrReplaceTempView("words")
    //
    //        // Do word count on DataFrame using SQL and print it
    //        val wordCountsDataFrame =
    //          spark.sql("select word, count(*) as total " +
    //            "from words " +
    //            "group by word " +
    //            "order by total desc")
    //        wordCountsDataFrame.show()
    //      }

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}