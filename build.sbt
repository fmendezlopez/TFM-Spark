name := "TFM-Spark"

version := "1.0"

scalaVersion := "2.11.7"

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.apache.commons" % "commons-vfs2" % "2.0",
  //"com.databricks" %% "spark-csv" % "1.4.0",
  "log4j" % "log4j" % "1.2.17",
  "com.github.tototoshi" %% "scala-csv" % "1.3.4",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0"
)
