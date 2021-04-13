//val dottyVersion = "3.0.0-M2"
val dottyVersion = "2.12.10"

lazy val root = project
  .in(file("."))
  .settings(
    name := "dotty-simple",
    version := "0.1.0",

    scalaVersion := dottyVersion,

    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
  )
