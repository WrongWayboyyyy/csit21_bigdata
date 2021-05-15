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

    resultDataSet
      // Пишем в BigQuery
      .write.format("bigquery")
      // Устанавливаем переписывание существующих файлов
      .mode(SaveMode.Overwrite)
      // Сохраняем как таблицу в файл по индивидуальному пути
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
      .csv("gs://mmmmonkey/data.csv") // Для запуска на клауде

    import ss.sqlContext.implicits._
    // Выполнение 1.1
    sortByParam(ss, df, "city_or_county")
    // Выполнение 1.2
    sortByParam(ss, df, "state")
    // Выполнение 1.3
    val dataSet = df.as[Case]

    dataSet
      // Предикат, заданный в условии
      .map(x => if (x.gun_stolen.toString.contains("Stolen")
        || x.gun_stolen.toString.contains("Unknown"))
          "Stolen || Unknown" else "Other")
      // Группируем по колонке value
      // TODO: Понять, как самостоятельно именовать колонки или каким образом Spark именует эти колонки
      .groupBy("value")
      // Считаем
      .count()
      // Сохраняем аналогично функции filterByParam
      .write.format("bigquery")
      .mode(SaveMode.Overwrite)
      .option("table", s"data.gun_stolen_output")
      .save()
  }
}


