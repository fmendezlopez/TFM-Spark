package es.uam.eps.tfm.fmendezlopez.allrecipes

import java.io.File

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

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
    val inputPath = args(0)
    val outputPath = args(1)

    val ingredients = readCSV(inputPath, "ingredients", Some(options), None)
    deleteIngredientsID0(ingredients, outputPath)

    val reviews = readCSV(inputPath, "reviews", Some(options), None)
    val recipes = readCSV(inputPath, "recipes", Some(options), None)
    deleteReviewsWithoutRecipe(reviews, recipes, outputPath)
  }

  def deleteIngredientsID0(ingredients: DataFrame, outputPath: String) = {
    println(s"Number of ingredients before: ${ingredients.count()}")
    val ingredientsFiltered = ingredients.filter(col("ID_INGREDIENT") =!= lit(0))
    println(s"Number of ingredients after: ${ingredientsFiltered.count()}")
    writeCSV(ingredients, outputPath, "ingredients", Some(options))
  }

  def deleteReviewsWithoutRecipe(reviews: DataFrame, recipes: DataFrame, outputPath: String) = {
    println(s"Number of reviews before: ${reviews.count()}")
    val validReviews = reviews.join(recipes, "ID_RECIPE").select(reviews.columns.map(reviews(_)):_*)
    println(s"Number of reviews after: ${validReviews.count()}")
    writeCSV(validReviews, outputPath, "reviews", Some(options))
  }
}
