import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col

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
  // Функция подсчета количества элементов по заданному фильтру
  def countByFilter(ss: SparkSession, df: DataFrame, requiredFilter: String): Unit = {
    import ss.sqlContext.implicits._
    // Получаем strong-typed DataSet[Case] из generic-typed DataFrame и отбираем данные по нужному фильтру
    val dataSet = df.as[Case].select(requiredFilter)
    // Превращаем каждый элемент типа Case в пару (Case,int) собираем по ключу и кэшируем
    val rdd = dataSet.rdd
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .cache()

    rdd
      .map(x => (x._2, x._1)) // Мэпим, чтобы ключ стоял на первой позиции
      .sortByKey(ascending = false) // Сортируем
      .map(x=> (x._2, x._1)) // Мэпим обратно
      .foreach(x => println(x)) // Выводим
  }
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO) // Пока не знаю, что это
    // TODO: Соединение с Google Cloud BigQuery
    // Раскоментить, когда будем загружать на Storage
//    if (args.length != 2) {
//      throw new IllegalArgumentException("Provide exactly 2 arguments")
//    }

   // val inputPath = args(0)
   // val outputPath = args(1)

    // Создаем новую сессию
    val ss = SparkSession
      .builder()
      .appName("CSIT 2021 Spark Application")
      .master("local[*]")
      .getOrCreate()
    // Считываем файл
    val dataFrame = ss.read
      .option("wholeFile", "true") // Магическая опция (немного даже не уверен, что нужна)
      .option("multiline","true") // Аналогично
      .option("header", "true") // Указываем, что в csv содержится заголовок
      .option("inferSchema", "true") // Аналогично 1 и 2))
      .option("quote", "\"") // Чтобы корректно читать nullField-ы в исходнои файле
      .option("escape", "\"") // Аналогично
      .option("encoding", "UTF-8") // На всякий случай
      .option("dateFormat", "yyyy-MM-dd") // Аналогично
      //.csv("gs://mmmmonkey/data.csv") Для запуска на клауде
      .csv("data.csv") // Для локального запуска
    // Вызываем наш метод
    countByFilter(ss, dataFrame, "state")
    // Для локального запуска
    //Thread.sleep(10 * 60 * 1000) //Для локального запуска, чтобы не рухнул c исключением
  }
}