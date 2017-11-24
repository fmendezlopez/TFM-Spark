package es.uam.eps.tfm.fmendezlopez

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by franm on 17/09/2017.
  */
object DifficultyProcessing {

  private var spark : SparkSession = _

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .appName("TFM-Spark")
      .getOrCreate()

    unionAllDatasets
  }

  def unionAllDatasets = {

    def printStats(chowhound : DataFrame, cookipedia : DataFrame, recipekey : DataFrame, saveur : DataFrame) = {
      println(s"Chowhound")
      println(s"\t-total recipes: ${chowhound.count}")
      println(s"\t-hard recipes: ${chowhound.filter("DIFFICULTY='hard'").count}")
      println(s"\t-medium recipes: ${chowhound.filter("DIFFICULTY='medium'").count}")
      println(s"\t-easy recipes: ${chowhound.filter("DIFFICULTY='easy'").count}")
      println(s"Cookipedia")
      println(s"\t-total recipes: ${cookipedia.count}")
      println(s"\t-hard recipes: ${cookipedia.filter("DIFFICULTY='hard'").count}")
      println(s"\t-medium recipes: ${cookipedia.filter("DIFFICULTY='medium'").count}")
      println(s"\t-easy recipes: ${cookipedia.filter("DIFFICULTY='easy'").count}")
      println(s"Recipekey")
      println(s"\t-total recipes: ${recipekey.count}")
      println(s"\t-hard recipes: ${recipekey.filter("DIFFICULTY='hard'").count}")
      println(s"\t-medium recipes: ${recipekey.filter("DIFFICULTY='medium'").count}")
      println(s"\t-easy recipes: ${recipekey.filter("DIFFICULTY='easy'").count}")
      println(s"Saveur")
      println(s"\t-total recipes: ${saveur.count}")
      println(s"\t-hard recipes: ${saveur.filter("DIFFICULTY='hard'").count}")
      println(s"\t-medium recipes: ${saveur.filter("DIFFICULTY='medium'").count}")
      println(s"\t-easy recipes: ${saveur.filter("DIFFICULTY='easy'").count}")
    }
    val options : Map[String, String] = Map(
      "sep" -> "|",
      "encoding" -> "UTF-8",
      "quote" -> ""
    )
    val schema = StructType(Seq(
      StructField("ID", LongType),
      StructField("PREP_TIME", IntegerType),
      StructField("COOK_TIME", IntegerType),
      StructField("TOTAL_TIME", IntegerType),
      StructField("#INGREDIENTS", IntegerType),
      StructField("#STEPS", IntegerType)
    ))
    /*
    val chowhound = SparkUtils.readCSV(
      "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\difficulty\\chowhound\\recipes.csv",
      Some(options),
      Some(schema))
    val cookipedia = SparkUtils.readCSV(
      "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\difficulty\\cookipedia\\recipes.csv",
      Some(options),
      Some(schema))
    val recipekey = SparkUtils.readCSV(
      "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\difficulty\\recipekey\\recipes.csv",
      Some(options),
      Some(schema))
    val saveur = SparkUtils.readCSV(
      "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\difficulty\\saveur\\recipes.csv",
      Some(options),
      Some(schema))

    //printStats(chowhound, cookipedia, recipekey, saveur)

    chowhound.printSchema()
    cookipedia.printSchema()
    recipekey.printSchema()
    saveur.printSchema()
    */
  }
}
