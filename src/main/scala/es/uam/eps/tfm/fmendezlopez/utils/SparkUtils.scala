package es.uam.eps.tfm.fmendezlopez.utils

import java.io.{File, FilenameFilter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by franm on 16/07/2017.
  */
object SparkUtils {

  def readCSV(path : String, name: String, options : Option[Map[String, String]], schema : Option[StructType]) : DataFrame = {
    val suffix = if(name.contains("csv")) "" else ".csv"
    val myPath = s"${path}${File.separator}${name}${suffix}"
    if(schema.isEmpty) {
      SparkSession.builder().getOrCreate().read
        .options(options.getOrElse(Map()))
        .format("csv")
        .option("header", true)
        .csv(myPath)
    }
    else {
      SparkSession.builder().getOrCreate().read
        .options(options.getOrElse(Map()))
        .format("csv")
        .schema(schema.get)
        .option("header", true)
        .csv(myPath)
    }
  }
  def writeCSV(df: DataFrame, path: String, name: String, options : Option[Map[String, String]]) = {
    val myPath = s"${path}${File.separator}${name}"
    df.coalesce(1).write
      .options(options.getOrElse(Map()))
      .format("com.databricks.spark.csv")
      .save(myPath)
    val file = new File(myPath)
    val csv = file.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.matches("^.*(.csv)$")
    }).head
    val suffix = if(name.contains("csv")) "" else ".csv"
    val newFile = new File(s"${path}${File.separator}${name}${suffix}")
    csv.renameTo(newFile)
    FileUtils.deleteDirectory(new File(myPath))
  }

  object sql{
    def min(col1: Column, col2: Column) = when(col1 <= col2, col1).otherwise(col2)
    def cosine(left: Row, right: Row, columns: Seq[String]): Double = {
      def mod(row: Row, columns: Seq[String]): Double = {
        scala.math.sqrt(columns map {name => {
          val value = row.getAs[Double](name)
          value * value
        }} sum)
      }
      val numerator = columns map {name => {
        val lVal = left.getAs[Double](name)
        val rVal = right.getAs[Double](name)
        lVal * rVal
      }} sum
      val lMod = mod(left, columns)
      val rMod = mod(right, columns)
      val denominator = lMod * rMod
      numerator / denominator
    }
  }

}
