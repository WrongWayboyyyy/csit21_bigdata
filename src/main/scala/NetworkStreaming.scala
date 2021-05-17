import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

object NetworkStreaming {
  def sortByParam(sparkSession: SparkSession, dataFrame: DataFrame, requiredFilter: String): Unit = {
    import sparkSession.sqlContext.implicits._
    // Парсим DataFrame как DataSet из элементов класса Case
    val dataSet = dataFrame.as[Case]

    val resultDataSet = dataSet
      // Группируем по требуемому признаку
      .groupBy(requiredFilter)
      // Собираем DataFrame по группам
      .count()
      // Сортируем по насчитанному значению
      .sort($"count".desc)
      // Ограничиваем десятью значениями
      .limit(10)
      // Сохраняем
      .cache()

    val query = resultDataSet.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]) : Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession
      .builder
      .appName("StructuredNetwork")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val pubsubDF = spark
      .readStream
      .format("pubsublite")
      .option("pubsublite.subscription", "projects/gun-shooting-analysis/subscriptions/mmmmonkey")
      .load()

    val encoderSchema = Encoders.product[Case].schema

    val result = pubsubDF.groupBy("city_or_county")
      .count()
      .sort($"count".desc)
      // Ограничиваем десятью значениями
      .limit(10)

    val query = result.writeStream
      .outputMode("update")
      .format("bigquery")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .option("table", "data.sortBy_streaming_output")
      .start()

    query.awaitTermination()
  }
}