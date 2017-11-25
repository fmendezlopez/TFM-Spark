package es.uam.eps.tfm.fmendezlopez.allrecipes

import java.io.File

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by franm on 19/11/2017.
  */
object PropagationRecommender {

  private var spark : SparkSession = _
  private lazy val DEFAULT_TRAINING_SIZE = 0.7f
  private lazy val DEFAULT_TEST_SIZE = 0.3f

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )
  val baseOutputPath = "./src/main/resources/output/recommendation/allrecipes/propagation"
  val baseInputPath = "./src/main/resources/input/recommendation/allrecipes/propagation"

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Allrecipes Recommender")
      .getOrCreate()
  }

  def preprocesser = {

    val recipes = SparkUtils.readCSV(baseInputPath, "recipes", Some(options), None)
    //println(recipes.count())
    //println(recipes.dropDuplicates("ID_RECIPE").count())
    val ingr = SparkUtils.readCSV(baseInputPath, "ingredients", Some(options), None)
    ingr.cache()
    println(s"Number of ingredients before: ${ingr.count()}")
    val ingredients = ingr.filter(col("ID_INGREDIENT") =!= lit(0))
    println(s"Number of ingredients after: ${ingredients.count()}")
    var oldFile = new File(s"${baseInputPath}${File.separator}ingredients_old.csv")
    var newFile = new File(s"${baseInputPath}${File.separator}ingredients.csv")
    newFile.renameTo(oldFile)
    SparkUtils.writeCSV(ingredients, baseInputPath, "ingredients", Some(options))

    val reviews = SparkUtils.readCSV(baseInputPath, "reviews", Some(options), None)
    reviews.cache()
    println(s"Number of reviews before: ${reviews.count()}")
    val validReviews = reviews.join(recipes, "ID_RECIPE").select(reviews.columns.map(reviews(_)):_*)
    println(s"Number of reviews after: ${validReviews.count()}")
    oldFile = new File(s"${baseInputPath}${File.separator}reviews_old.csv")
    newFile = new File(s"${baseInputPath}${File.separator}reviews.csv")
    newFile.renameTo(oldFile)
    SparkUtils.writeCSV(validReviews, baseInputPath, "reviews", Some(options))
  }

  def contentAnalyzer = {
    object sampling{
      def percentSampling(user_recipe: DataFrame) = {
        val user_reviews = user_recipe.filter("")
      }
    }
  }
}
