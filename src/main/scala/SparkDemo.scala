import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, col}

// Класс для парсинга (содержит все те поля, что нужны для обработки DataFrame)
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

  def sortByParam(sparkSession: SparkSession, dataFrame: DataFrame, requiredFilter: String): Unit = {
    import sparkSession.sqlContext.implicits._

    var dataSet = dataFrame.as[Case]

    var resultDataSet = dataSet
      .groupBy(requiredFilter)
      .count()
      .sort($"count".desc)
      .limit(10)
      .cache()

    resultDataSet
      .write.format("bigquery")
      .mode(SaveMode.Overwrite)
      .option("table", s"data.sortBy_${requiredFilter}_output")
      .save()
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO) // Пока не знаю, что это
    SparkSession
    // Создаем новую сессию
    val ss = SparkSession
      .builder()
      .appName("CSIT 2021 Spark Application")
      .master("local[*]")
      .getOrCreate()

    val bucket = "csit21_tempbucket"
    ss.conf.set("temporaryGcsBucket", bucket)
    // Считываем файл
    val df = ss.read
      .option("wholeFile", "true") // Магическая опция (немного даже не уверен, что нужна)
      .option("multiline", "true") // Аналогично
      .option("header", "true") // Указываем, что в csv содержится заголовок
      .option("inferSchema", "true") // Аналогично 1 и 2))
      .option("quote", "\"") // Чтобы корректно читать nullField-ы в исходнои файле
      .option("escape", "\"") // Аналогично
      .option("encoding", "UTF-8") // На всякий случай
      .option("dateFormat", "yyyy-MM-dd") // Аналогично
      .csv("gs://mmmmonkey/data.csv") //Для запуска на клауде
    //.csv("data.csv") // Для локального запуска

    import ss.sqlContext.implicits._

    sortByParam(ss, df, "city_or_county")
    sortByParam(ss, df, "state")

    val dataSet = df.as[Case]

    val resultDataSet: Unit = dataSet
      .map(x => if (x.gun_stolen.toString.contains("Stolen")
        || x.gun_stolen.toString.contains("Unknown"))
        "Stolen || Unknown" else "Other")
      .groupBy("value")
      .count()
      .write.format("bigquery")
      .mode(SaveMode.Overwrite)
      .option("table", s"data.gun_stolen_output")
      .save()

  }
}


