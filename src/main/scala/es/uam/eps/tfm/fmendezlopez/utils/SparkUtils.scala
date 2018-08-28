package es.uam.eps.tfm.fmendezlopez.utils

import java.io.{File, FilenameFilter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable
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

  def minmaxNormalize(oldMin: Double, oldMax: Double, newMin: Double, newMax: Double, value: Double): Double = {
    ((value - oldMin) / (oldMax - oldMin)) * (newMax - newMin) + newMin
  }

  def floorOrCeil(value: Double): Double = {
    val interval: Double = math.ceil(value) - math.floor(value)
    if(interval - value > 0.5f) math.floor(value)
    else math.ceil(value)
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

  object evaluation {
    def regressionEvaluation(df: DataFrame, col1: String, col2: String): (String, String, String, String) = {
      val regressionMetrics = new RegressionMetrics(df
        .select(
          col(col1).cast(DoubleType),
          col(col2).cast(DoubleType))
        .rdd.map(r => (r.getDouble(0), r.getDouble(1))))

      (
        col2,
        f"${regressionMetrics.meanSquaredError}%.3f",
        f"${regressionMetrics.meanAbsoluteError}%.3f",
        f"${regressionMetrics.rootMeanSquaredError}%.3f"
      )
    }

    def binaryEvaluation(df: DataFrame, col1: String, col2: String, threshold1: Double, threshold2: Double):
    Seq[String] = {
      val binaryMetrics = new BinaryClassificationMetrics(df
        .select(
          col(col1).cast(DoubleType),
          col(col2).cast(DoubleType))
        .rdd.map(r => (if(r.getDouble(0) < threshold1) 0 else 1, if(r.getDouble(1) < threshold2) 0 else 1)))

      val result: Seq[String] =
        Seq(
          col2,
          f"${binaryMetrics.areaUnderPR()}%.3f",
          f"${binaryMetrics.areaUnderROC()}%.3f") ++
          binaryMetrics.precisionByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")) ++
          binaryMetrics.recallByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")) ++
          binaryMetrics.fMeasureByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")
          )
      result
    }

    def manualEvaluation(df: DataFrame, col1: String, col2: String, threshold1: Double, threshold2: Double): Seq[Double] = {
      val typed = df
        .withColumn("type",
          when(col(col2) >= lit(threshold2), when(col(col1) >= lit(threshold1), lit("TP")).otherwise(lit("FP")))
            .otherwise(when(col(col1) < lit(threshold1), lit("TN")).otherwise(lit("FN")))
        )

      val tp = typed.filter("type = 'TP'").count().toFloat
      val tn = typed.filter("type = 'TN'").count().toFloat
      val fp = typed.filter("type = 'FP'").count().toFloat
      val fn = typed.filter("type = 'FN'").count().toFloat

      val ACC = (tp + tn) / (tp + tn + fp + fn)
      val PPV: Float = tp / (tp + fp)
      val NPV: Float = tn / (tn + fn)
      val TPR: Float = tp / (tp + fn)
      val TNR: Float = tn / (tn + fp)
      val FPR: Float = fp / (fp + tn)
      val FNR: Float = fn / (fn + tp)

      Seq(
        tp,
        tn,
        fp,
        fn,
        ACC,
        PPV,
        NPV,
        TPR,
        TNR,
        FPR,
        FNR)
    }

    def precisionAtK(df: DataFrame, userCol: String, recipeCol: String, col1: String, col2: String, values: Seq[Int]): Seq[Float] = {
      val result: mutable.Seq[Float] = mutable.Seq.fill(values.length)(0)
      var nusers = 0
      val users = df.select(userCol).distinct().collect()
      println(s"number of users: ${users.length}")
      users.map(row => {
        val id_user = row.getInt(0)
        val recipes = df.filter(s"$userCol = $id_user")
        val sorted1 = recipes.orderBy(desc(col1)).select(recipeCol).collect().map(_.getInt(0))
        val sorted2 = recipes.orderBy(desc(col2)).select(recipeCol).collect().map(_.getInt(0))
        var i = 0
        values.foreach(k => {
          /*
          val rank: Seq[(Int, Int)] = zip.take(k).takeWhile({ case (a, b) => a == b })
          val value = rank.length.toFloat / k.toFloat
          println(s"value: $value")
          result(i) += value
          i += 1
          */
          val intersection = sorted1.take(k).intersect(sorted2.take(k))
          val precision = intersection.length.toFloat / k.toFloat
          result(i) = result(i) + precision
          i += 1
        })
        nusers += 1
        println(s"user $nusers")
        //println(result.mkString(","))
      })
      result.map(_ / nusers)
    }
  }
}
