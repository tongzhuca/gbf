name := "simple-bin"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"


libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.0"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.4.0"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"