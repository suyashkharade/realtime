name := "import-csv"

version := "0.1"

scalaVersion := "2.11.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.12"
