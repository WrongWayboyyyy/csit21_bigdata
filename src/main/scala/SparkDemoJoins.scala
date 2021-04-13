import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkDemoJoins {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val ss = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()
    import ss.implicits._

    val peopleDF = Seq(
      ("andrea", "medellin"),
      ("rodolfo", "medellin"),
      ("abdul", "bangalore")
    ).toDF("first_name", "city")

    peopleDF.show()
//      .repartition(5, $"city")

//    println(peopleDF.rdd.getNumPartitions)

    val citiesDF = Seq(
      ("medellin", "colombia", 2.5),
      ("bangalore", "india", 12.3)
    ).toDF("city", "country", "population")

    citiesDF.show()
//
    val joined =
      peopleDF.as("people").join(citiesDF.as("cities")
      , peopleDF("city") <=> citiesDF("city"), "inner")
        .filter($"people.city" === "medellin")
        .select("first_name", "people.city", "country", "population")

//    joined.explain(mode="formatted")
    joined.explain(extended = true)
//      joined.show()
//
//    Thread.sleep(10 * 60 * 1000)
  }
}