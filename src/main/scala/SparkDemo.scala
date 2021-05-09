import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO) // Пока не знаю, что это

    val sc = new SparkContext("local[*]", "SparkDemo") // Создание нового контекста
    var lines = sc.textFile("data.csv") // Должно быть раскоменчено, когда будем работать с реальным файлом
    val dataTypeRdd = lines
      // Делим по \n, чтобы получить как бы отдельную колонку данных,
      // при этом выкидываем первую, которая содержит сами значения колонок (drop)
      .flatMap(line => line.split("\n"))
      .cache()
    // Собираем массив возможных типов данных (штат, регион и т.д)
    val columns = dataTypeRdd.first().split(',').map(x => x.trim())

    val filteredRdd = lines
      // Преобразуем каждый получившийся элемент в лист, элементами которого
      // являются разделенный по символу конца строки строки (по сути элементы таблицы)
      .map(line => line.split("\n"))
      // Разделим массив на блоки по разделителю |, сохраняя при этом его, чтобы
      // понимать, какие строки являются первым блоком входных данных, а какие нет.
      .map(array => array.flatMap(line => line.split("(?=\\|\\|)")))
      // Разделим первый блок данных (до первого вхождения |) по разделителю
      // запятой
      .map(array => array.flatMap(line =>
        if (!line.startsWith("\\|")) line.split(',') else Array(line)))
      // Каждую строку приведем к кортежу (строка, число), где число
      // будет означать индекс строки в текущем блоки
      .flatMap(array => array.map(line => (line, array.indexOf(line))))
      // Приведем кортеж вида (строка, число) к виду (строка, строка), где
      // вторая строка указывает, к какому типу данных относится текущее значение
      .map(pair => (pair._1, columns(math.min(pair._2, 28))))
      // Отделяем все, кроме необходимого
      .filter(pair => pair._2.equals("state") &&
        !pair._1.matches(".*[0-9]+.*") &&
        !pair._1.equals(""))
      // Знание о том, что это необходимый нам фильтр теперь бесполезно, з
      // аменим его на количество вхождений этого элемента
      .map(a => (a._1, 1))
      // Выполняем reduceByKey (лучше об этом почитать) и получаем количество
      // вхождений каждой строки в нашем датасете
      .reduceByKey(_ + _)
      // Кэшируем данные для дальнейшего использования
      .cache()
    // Сортируем по убыванию и выводим (зачем нужны эти 2 отображения я не совсем понял)
    filteredRdd
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(x => println(x))
    //Thread.sleep(10 * 60 * 1000) Для локального запуска, чтобы не рухнуло
    // С исключением
  }
}