package es.uam.eps.tfm.fmendezlopez.allrecipes

import java.io.File

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by franm on 25/11/2017.
  */
object Preprocesser {

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Preprocessing")
      .getOrCreate()

    val inputPath = "C:\\Users\\franm\\IdeaProjects\\TFM\\Extractor\\src\\main\\resources\\input\\dataset"
    val outputPath = "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\upgraded_dataset"
    //val inputPath = args(0)
    //val outputPath = args(1)

    /*
    val reviews = readCSV(inputPath, "reviews", Some(options), None)
    val recipes = readCSV(inputPath, "recipes", Some(options), None)
    deleteReviewsWithoutRecipe(reviews, recipes, outputPath)
    */

    val fav = readCSV(inputPath, "favourites", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("fav"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID"))
    val publications = readCSV(inputPath, "publications", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("recipes"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID").as("USER_ID"))
    val madeit = readCSV(inputPath, "madeit", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("madeit"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID").as("USER_ID"))
    val reviews_aux = readCSV(inputPath, "reviews", Some(options), None)
    val reviews = reviews_aux
      .withColumn("RECIPE_TYPE", lit("review"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("AUTHOR_ID").as("USER_ID"))
    val ingredients_aux = readCSV(inputPath, "ingredients", Some(options), None)
    val ingredients = ingredients_aux
      .filter(col("ID") =!= lit(0))
      .select(Seq(col("ID").as("ID_INGREDIENT")) ++ ingredients_aux.columns.filter(_ != "ID").map(col) :_*)
    val recipes_aux = readCSV(inputPath, "recipes", Some(options), None)
    val recipes = recipes_aux.select(Seq(col("ID").as("RECIPE_ID")) ++ recipes_aux.columns.filter(_ != "ID").map(col) :_*)
    deleteReviewsWithoutRecipe(reviews_aux, recipes_aux, "")
    val user_recipes = fav.union(publications).union(madeit).union(reviews)

    writeCSV(user_recipes, outputPath, "user-recipe", Some(options))
    writeCSV(ingredients, outputPath, "ingredients", Some(options))
    writeCSV(recipes, outputPath, "recipes", Some(options))
  }

  def deleteReviewsWithoutRecipe(reviews: DataFrame, recipes: DataFrame, outputPath: String) = {
    println(s"Number of reviews before: ${reviews.count()}")
    val validReviews = reviews
      .join(recipes, reviews("RECIPE_ID") === recipes("ID"))
      .select(reviews.columns.map(reviews(_)):_*)
    println(s"Number of reviews after: ${validReviews.count()}")
    //writeCSV(validReviews, outputPath, "reviews", Some(options))
  }
}
