package es.uam.eps.tfm.fmendezlopez.utils

import java.io.{File, FilenameFilter, PrintWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
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
    def floorOrCeil(value: Column): Column = {
      val interval = ceil(value) - floor(value)
      when((interval - value).cast(DoubleType) > lit(0.5).cast(DoubleType), floor(value).cast(IntegerType))
        .otherwise(ceil(value).cast(IntegerType))
    }
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

    def binaryEvaluation(df: DataFrame, label: String, predictionLabel: String, threshold1: Double, threshold2: Double):
    Seq[String] = {
      //RDD[prediction, label-real value]
      val rdd: RDD[(Double, Double)] = df
        .select(
          col(predictionLabel).cast(DoubleType),
          col(label).cast(DoubleType))
        .rdd.map(r => (if(r.getDouble(0) < threshold1) 0 else 1, if(r.getDouble(1) < threshold2) 0 else 1))
      val binaryMetrics = new BinaryClassificationMetrics(rdd)

      val result: Seq[String] =
        Seq(
          predictionLabel,
          f"${binaryMetrics.areaUnderPR()}%.3f",
          f"${binaryMetrics.areaUnderROC()}%.3f") ++
          binaryMetrics.precisionByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")) ++
          binaryMetrics.recallByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")) ++
          binaryMetrics.fMeasureByThreshold.collect().flatMap(t => Seq(f"${t._2}%.3f")
          )
      result
    }

    def manualEvaluation(df: DataFrame, col1: String, col2: String, threshold1: Double, threshold2: Double): Seq[String] = {
      df.cache()
      val typed = df
        .withColumn("type",
          when(col(col2) >= lit(threshold2), when(col(col1) >= lit(threshold1), lit("TP")).otherwise(lit("FP")))
            .otherwise(when(col(col1) < lit(threshold1), lit("TN")).otherwise(lit("FN")))
        )
      //typed.filter("USER_ID = '855475' AND RECIPE_ID='18509'").show()

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
      df.unpersist()
      Seq(
        col2,
        f"$tp%.3f",
        f"$tn%.3f",
        f"$fp%.3f",
        f"$fn%.3f",
        f"$ACC%.3f",
        f"$PPV%.3f",
        f"$NPV%.3f",
        f"$TPR%.3f",
        f"$TNR%.3f",
        f"$FPR%.3f",
        f"$FNR%.3f",
        {if(FPR == 0) "MAX" else f"${TPR / FPR}%.3f"}
      )
    }

    def precisionAtK(
                      df: DataFrame, userCol: String, recipeCol: String,
                      col1: String, col2: String, values: Seq[Int],
                      thresholdRelevant: Double
                    ): Seq[String] = {
      val result: mutable.Seq[Float] = mutable.Seq.fill(values.length)(0)
      var nusers = 0
      df.cache()
      ///val users = df.select(userCol).distinct().collect()
      //println(s"number of users: ${users.length}")
      val k = 5
      val map: Map[Int, String] = values.flatMap(k => {
        val precision = df
          .select(
            col(userCol),
            col(col1),
            col(col2),
            row_number().over(Window.partitionBy(userCol).orderBy(col2)).as("ROW_NUM")
          )
          .filter(s"ROW_NUM <= $k")
          .withColumn("IS_RELEVANT", when(col(col1) >= lit(thresholdRelevant), lit(1)).otherwise(lit(0)))
          .groupBy("USER_ID").agg((sum(col("IS_RELEVANT")) / lit(k)).as(s"PU@$k"))
          .select(avg(s"PU@$k").as(s"P@$k"))
          .rdd.collect().head.getDouble(0)
        Map(k -> f"$precision%.3f")
      }).toMap
      df.unpersist()
      Seq(col2) ++ map.values.toSeq
      /*
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
      */
      //result.map(_ / nusers)
    }
  }
}
