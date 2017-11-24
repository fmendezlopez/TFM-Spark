package es.uam.eps.tfm.fmendezlopez

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by franm on 22/09/2017.
  */
object Allrecipes {

  def main(args: Array[String]): Unit = {
    getStats
  }
  def getStats = {
    val options : Map[String, String] = Map(
      "sep" -> "|",
      "encoding" -> "UTF-8"
    )

    /*
    val users = SparkUtils.readCSV(
      "C:\\Users\\franm\\Desktop\\exe\\extractor_jar\\Extractor\\output\\stage4\\1\\FRAN-LAPTOP\\users.csv",
      Some(options),
      None)

    val recipes = SparkUtils.readCSV(
      "C:\\Users\\franm\\Desktop\\exe\\extractor_jar\\Extractor\\output\\stage4\\1\\FRAN-LAPTOP\\recipes.csv",
      Some(options),
      None)

    val reviews = SparkUtils.readCSV(
      "C:\\Users\\franm\\Desktop\\exe\\extractor_jar\\Extractor\\output\\stage4\\1\\FRAN-LAPTOP\\reviews.csv",
      Some(options),
      None)

    val user_recipe = SparkUtils.readCSV(
      "C:\\Users\\franm\\Desktop\\exe\\extractor_jar\\Extractor\\output\\stage4\\1\\FRAN-LAPTOP\\user-recipe.csv",
      Some(options),
      None)


    val schemaUsers = StructType(Seq(
      StructField("ID", LongType),
      StructField("PREP_TIME", IntegerType),
      StructField("COOK_TIME", IntegerType),
      StructField("TOTAL_TIME", IntegerType),
      StructField("#INGREDIENTS", IntegerType),
      StructField("#STEPS", IntegerType)
    ))


    val users_rec_30 = user_recipe
      .groupBy("ID_AUTHOR")
      .count()
      .filter("count > 30")

    val users_rev_30 = reviews
      .groupBy("ID_AUTHOR")
      .count()
      .filter("count > 30")
      .select(col("ID_AUTHOR").as("REV_ID_AUTHOR"))

    val rev_rec_30 = users_rec_30.join(users_rev_30, users_rec_30("ID_AUTHOR") === users_rev_30("REV_ID_AUTHOR"), "inner")

    //1616
    println(s"#users with more than 30 recipes: ${users_rec_30.count}")
    println(s"#users with more than 30 ratings: ${users_rev_30.count()}")
    println(s"#users with more than 30 ratings and more than 30 recipes: ${rev_rec_30.count()}")
    */
  }
}
