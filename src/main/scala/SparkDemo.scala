import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
case class Case(incident_id: Option[Long],
                date: Option[String],
                state : Option[String],
                city_or_county: Option[String],
                address : Option[String],
                n_killed : Option[Long],
                n_injured : Option[Long],
                incident_url : Option[String],
                source_url : Option[String],
                incident_url_fields_missing : Option[Boolean],
                congressional_district : Option[Long],
                gun_stolen: Option[String],
                gun_type: Option[String],
                incident_characteristics: Option[String],
                latitude: Option[Double],
                location_description: Option[String],
                longitude: Option[Double],
                n_guns_involved: Option[Long],
                notes: Option[String],
                participant_age: Option[String],
                participant_age_group: Option[String],
                participant_gender: Option[String],
                participant_name: Option[String],
                participant_relationship: Option[String],
                participant_status: Option[String],
                participant_type: Option[String],
                sources: Option[String],
                state_house_district: Option[Long],
                state_senate_district: Option[Long]
               )

object SparkDemo {
  def countByFilter(ss: SparkSession, df: DataFrame, requiredFilter: String): Unit = {
    import ss.sqlContext.implicits._

    val dataSet = df.as[Case].select(requiredFilter)
    val rdd = dataSet.rdd
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .cache()

    rdd
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x=> (x._2, x._1))
      .foreach(x => println(x))
  }
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO) // Пока не знаю, что это
//    if (args.length != 2) {
//      throw new IllegalArgumentException("Provide exactly 2 arguments")
//    }

   // val inputPath = args(0)
   // val outputPath = args(1)


    val ss = SparkSession
      .builder()
      .appName("CSIT 2021 Spark Application")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = ss.read
      .option("wholeFile", "true")
      .option("multiline","true")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("encoding", "UTF-8")
      .option("dateFormat", "yyyy-MM-dd")
      //.csv("gs://mmmmonkey/data.csv")
      .csv("data.csv")

    countByFilter(ss, dataFrame, "state")

    //Thread.sleep(10 * 60 * 1000) //Для локального запуска, чтобы не рухнул c исключением
  }
}