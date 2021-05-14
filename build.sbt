//val dottyVersion = "3.0.0-M2"
val dottyVersion = "2.12.10"

lazy val root = project
  .in(file("."))
  .settings(
        name := "dotty-simple",
        version := "0.1.0",

          libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
          // https://mvnrepository.com/artifact/org.apache.spark/s..
          libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1",
          // https://mvnrepository.com/artifact/org.apache.spark/s..
          libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1",
          // https://mvnrepository.com/artifact/org.apache.spark/s..
          //libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1",
          libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1",
          // https://mvnrepository.com/artifact/org.apache.bahir/s..
          libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0",
          libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.16.1",
          libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.70.0",
          // https://mvnrepository.com/artifact/com.google.cloud/g..
          //libraryDependencies += "com.google.cloud" % "pubsublite-spark-sql-streaming" % "0.1.0",
          libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.56.0",
          // https://mvnrepository.com/artifact/com.google.cloud.s..
          libraryDependencies += "com.google.cloud.spark" % "spark-bigquery_2.12" % "0.16.1",
          // https://mvnrepository.com/artifact/io.grpc/grpc-netty
          libraryDependencies += "io.grpc" % "grpc-netty" % "1.35.0",
          // https://mvnrepository.com/artifact/io.netty/netty-han..
          libraryDependencies += "io.netty" % "netty-handler" % "4.1.52.Final",
          // https://mvnrepository.com/artifact/io.netty/netty-tcn..
          libraryDependencies += "io.netty" % "netty-tcnative-boringssl-static" % "2.0.34.Final",

  )