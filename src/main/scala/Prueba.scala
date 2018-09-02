import java.io.File

import es.uam.eps.tfm.fmendezlopez.allrecipes.{NonSupervisedRecommender, NonSupervisedRecommender_old}
import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by franm on 20/06/2017.
  */
object Prueba {

  private var spark : SparkSession = _

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
        .master("local[*]")
      .appName("SparkSessionZipsExample")
      .getOrCreate()

    val path = "C:\\Users\\franm\\IdeaProjects\\TFM\\Extractor\\src\\main\\resources\\input\\dataset"
    val rev = SparkUtils.readCSV(path, "reviews", Some(options), None)
      .dropDuplicates("ID")
    SparkUtils.writeCSV(rev, path, "reviews_dupl", Some(options))
  }

  def ex1 = {
    val data = spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq(1, 100, 5, 5)),
      Row.fromSeq(Seq(1, 200, 5, 2)),
      Row.fromSeq(Seq(1, 300, 3, 5)),
      Row.fromSeq(Seq(1, 400, 4, 4)),
      Row.fromSeq(Seq(1, 500, 1, 2)),
      Row.fromSeq(Seq(2, 100, 5, 5)),
      Row.fromSeq(Seq(2, 200, 5, 5)),
      Row.fromSeq(Seq(2, 300, 5, 5)),
      Row.fromSeq(Seq(2, 400, 5, 4)),
      Row.fromSeq(Seq(2, 500, 4, 5))
    ))

    val schema = StructType(Seq(
      StructField("ID_USER", IntegerType),
      StructField("ID_RECIPE", IntegerType),
      StructField("RATING", IntegerType),
      StructField("PREDICTION", IntegerType)
    ))
    val df = spark.sqlContext.createDataFrame(data, schema)
    df.select("ID_USER").distinct().collect().foreach(row => {
      val id_user = row.getInt(0)
      println(s"user: $id_user")
      val recipes = df.filter(s"ID_USER = $id_user")
      val sorted1 = recipes.orderBy(desc("RATING")).select("ID_RECIPE").collect().map(_.getInt(0))
      val sorted2 = recipes.orderBy(desc("PREDICTION")).select("ID_RECIPE").collect().map(_.getInt(0))
      val zip: Seq[(Int, Int)] = sorted1.zip(sorted2)
      (1 to 5).foreach(k => {
        val precision = zip.take(k).takeWhile({case(a, b) => a == b})
        println(s"Precision@$k: ${precision.length.toFloat / k.toFloat}")
      })
    })

    val seq = SparkUtils.evaluation.precisionAtK(df, "ID_USER", "ID_RECIPE", "RATING", "PREDICTION", Seq.range(1, 5), 3)
    seq.foreach(println)
  }

  def pca = {
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)

    val result = pca.transform(df).select("pcaFeatures")
    result.show(false)
  }

  def prueba = {
    val data = spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq(0, 100, 4)),
      Row.fromSeq(Seq(0, 101, 2)),
      Row.fromSeq(Seq(1, 100, 3)),
      Row.fromSeq(Seq(1, 102, 8)),
      Row.fromSeq(Seq(2, 101, 5)),
      Row.fromSeq(Seq(2, 103, 9))
    ))
    val schema = StructType(Seq(
      StructField("ID_USER", IntegerType),
      StructField("ID_INGREDIENT", IntegerType),
      StructField("times", IntegerType)
    ))
    val stats = spark.createDataFrame(data, schema)
    val df = stats
      .groupBy("ID_USER")
      .pivot("ID_INGREDIENT")
      .sum("times")
    df.na.fill(0).show()
  }

  def computeNutritionTest = {
    val dataU = spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq(0, 100)),
      Row.fromSeq(Seq(0, 101)),
      Row.fromSeq(Seq(1, 100)),
      Row.fromSeq(Seq(1, 102)),
      Row.fromSeq(Seq(2, 101)),
      Row.fromSeq(Seq(2, 103))
    ))
    val schemaU = StructType(Seq(
      StructField("ID_USER", IntegerType),
      StructField("ID_RECIPE", IntegerType)
    ))
    val user_recipe = spark.createDataFrame(dataU, schemaU)

    val dataN = spark.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq(100, 1, 3, 0, 4)),
      Row.fromSeq(Seq(101, 3, 11, 1, 0)),
      Row.fromSeq(Seq(102, 1, 10, 3, 1)),
      Row.fromSeq(Seq(103, 0, 1, 1, 2))
    ))
    val schemaN = StructType(Seq(
      StructField("ID_RECIPE", IntegerType),
      StructField("nutr1", IntegerType),
      StructField("nutr2", IntegerType),
      StructField("nutr3", IntegerType),
      StructField("nutr4", IntegerType)
    ))
    val nutr = spark.createDataFrame(dataN, schemaN)

    //val res = computeNutrition(user_recipe, nutr)
    //res.show(100, false)
    user_recipe.filter("ID_USER = 0").show()
  }

  def computeNutrition(user_recipe: DataFrame, nutrition: DataFrame): DataFrame = {
    val nutrition_as1 = nutrition.withColumnRenamed("ID_RECIPE", "RECIPE")
    val user_nutrition = user_recipe.join(nutrition_as1, user_recipe("ID_RECIPE") === nutrition_as1("RECIPE"))
      .select(
        (Seq(
          user_recipe("ID_USER"),
          user_recipe("ID_RECIPE")
        ) ++ nutrition_as1.columns.filterNot(_ == "RECIPE").map(col(_))):_*
      )
    user_nutrition.show()
    val agg = user_nutrition.groupBy("ID_USER").sum(user_nutrition.columns.filterNot(Seq("ID_RECIPE", "ID_USER") contains _):_*)
    agg
      .select((agg.columns.map(name =>{
        if(name != "ID_USER")
          col(name).as(name.substring(name.indexOf('(') + 1, name.indexOf(')')))
        else
          col(name)
      }
      )):_*)
  }

  def detectNegatives = {
    val ingredients = "C:\\Users\\franm\\Desktop\\exe\\resources\\input\\csv\\authors.csv"
    val df: DataFrame = spark.read
      .option("sep", "|")
      .format("csv")
      .option("header", true)
      .csv(ingredients)
    df.printSchema()
    //df.groupBy("ID_RECIPE").count().
    println(df.filter("ID_USER = '0' OR ID_USER = '-1'").count())
  }

  def filterRecipes = {
    val inputPath = "./src/main/resources/input/recipes.csv"
    val outputPath = "./src/main/resources/output/recipes_filtered.csv"

    val df: DataFrame = spark.read
      .option("sep", "|")
      .format("csv")
      .option("header", true)
      .csv(inputPath)
    val dfRes = df
      .filter(col("ID_AUTHOR") =!= lit(0))
      .dropDuplicates(Seq("ID_RECIPE", "ID_AUTHOR"))

    dfRes.coalesce(1).write
      .option("sep", "|")
      .option("header", true)
      .format("csv")
      .save(s"${outputPath}")
  }

  def filterUsers = {
    val inputPath = "./src/main/resources/input/user_url.csv"
    val outputPath_authors = "./src/main/resources/output/authors.csv"
    val outputPath_reviewers = "./src/main/resources/output/reviewers.csv"
    val outputPath_authors_grouped = "./src/main/resources/output/authors_grouped.csv"
    val outputPath_reviewers_grouped = "./src/main/resources/output/reviewers_grouped.csv"

    val df: DataFrame = spark.read
      .option("sep", "|")
      .format("csv")
      .option("header", true)
      .csv(inputPath)

    val dfFiltered = df
        .filter(col("ID_USER") =!= lit(0))
    dfFiltered.printSchema()

    val dfAuthors = dfFiltered
      .filter(col("ROLE") === lit("author"))
      .dropDuplicates(Seq("ID_RECIPE", "ID_USER"))
      .groupBy("ID_USER").count()
      .select(
        dfFiltered.col("ID_USER").as("ID_USER2"),
        col("count").as("RECIPE_COUNT"),
        lit("author").as("ROLE2")
      )

    val dfReviewers = dfFiltered
      .filter(col("ROLE") === lit("reviewer"))
      .groupBy("ID_USER").count()
      .select(
        dfFiltered.col("ID_USER").as("ID_USER2"),
        col("count").as("RECIPE_COUNT"),
        lit("reviewer").as("ROLE2")
      )

    val dfResA = dfFiltered.join(
      dfAuthors,
      dfFiltered.col("ID_USER") === dfAuthors.col("ID_USER2") &&
        dfFiltered.col("ROLE") === dfAuthors.col("ROLE2"),
      "inner"
    )
      .select(dfFiltered.columns.map(col(_))
        :+ dfAuthors.col("RECIPE_COUNT")
        : _*)
      .dropDuplicates(Seq("ID_RECIPE", "ID_USER"))
      .orderBy(desc("RECIPE_COUNT"))

    val dfResR = dfFiltered.join(
      dfReviewers,
      dfFiltered.col("ID_USER") === dfReviewers.col("ID_USER2") &&
        dfFiltered.col("ROLE") === dfReviewers.col("ROLE2"),
      "inner"
    )
      .select(dfFiltered.columns.map(col(_))
        :+ dfReviewers.col("RECIPE_COUNT")
        : _*)
      .orderBy(desc("RECIPE_COUNT"))

    val dfResAGrouped = dfResA.dropDuplicates("ID_USER").orderBy(desc("RECIPE_COUNT"))
    val dfResRGrouped = dfResA.dropDuplicates("ID_USER").orderBy(desc("RECIPE_COUNT"))

    dfResA.coalesce(1).write
      .option("sep", "|")
      .option("header", true)
      .format("csv")
      .save(s"${outputPath_authors}")

    dfResAGrouped.coalesce(1).write
      .option("sep", "|")
      .option("header", true)
      .format("csv")
      .save(s"${outputPath_authors_grouped}")

    dfResR.coalesce(1).write
      .option("sep", "|")
      .option("header", true)
      .format("csv")
      .save(s"${outputPath_reviewers}")

    dfResRGrouped.coalesce(1).write
      .option("sep", "|")
      .option("header", true)
      .format("csv")
      .save(s"${outputPath_reviewers_grouped}")
  }
}
