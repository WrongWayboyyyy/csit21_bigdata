//val dottyVersion = "3.0.0-M2"
val dottyVersion = "2.12.10"

val sparkVersion = "3.1.1"
val kafkaVersion = "2.4.0"


lazy val root = project
  .in(file("."))
  .settings(
    name := "dotty-simple",
    version := "0.1.0",

    resolvers += "Bintray Maven Repository" at "https://dl.bintray.com/spark-packages/maven",
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    // https://mvnrepository.com/artifact/org.apache.spark/s..
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    // https://mvnrepository.com/artifact/org.apache.spark/s..
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    // https://mvnrepository.com/artifact/org.apache.spark/s..
    //libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion,
    // https://mvnrepository.com/artifact/org.apache.bahir/s..
    libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.70.0",
    // https://mvnrepository.com/artifact/com.google.cloud/g..
      libraryDependencies += "com.google.cloud" % "google-cloud-pubsublite" % "0.14.1",
    libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.112.0",
    libraryDependencies += "com.google.cloud.spark" % "spark-bigquery-with-dependencies_2.12" % "0.20.0",
      // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
  )