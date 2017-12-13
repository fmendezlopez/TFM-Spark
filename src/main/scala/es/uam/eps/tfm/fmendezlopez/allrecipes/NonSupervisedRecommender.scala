package es.uam.eps.tfm.fmendezlopez.allrecipes

import java.io.File

import com.github.tototoshi.csv.CSVWriter
import es.uam.eps.tfm.fmendezlopez.utils.{CSVManager, SparkUtils}
import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RankingMetrics, RegressionMetrics}
import org.apache.spark.rdd.RDD

/**
  * Created by franm on 11/10/2017.
  */
object NonSupervisedRecommender {

  private var spark : SparkSession = _
  private lazy val DEFAULT_TRAINING_SIZE = 0.7f
  private lazy val DEFAULT_TEST_SIZE = 0.3f

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )
  val baseOutputPath = "./src/main/resources/output/recommendation/allrecipes/nonsupervised"
  val baseInputPath = "./src/main/resources/output/recommendation/allrecipes/preprocessing"
  val datasetPath = s"${baseOutputPath}${File.separator}preprocessed"
  val trainingPath = s"${baseOutputPath}${File.separator}training"
  val testPath = s"${baseOutputPath}${File.separator}test"

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Allrecipes Recommender")
      .getOrCreate()

    //contentAnalyzer
    //profileLearner
    //filteringComponent
    evaluate
  }

    /*Rename example*/
  /*
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
    ingr.unpersist()
  */

  def contentAnalyzer = {
    def getValidRecipes(recipes: DataFrame, ingredients: DataFrame, nutrition: DataFrame) : DataFrame = {
      println(s"Recipes size 1: ${recipes.count()}")
      val nutr_ing = ingredients/*.select(col("ID_RECIPE").as("RECIPE"))*/
        .join(nutrition, "ID_RECIPE")
        .select(col("ID_RECIPE").as("RECIPE")).distinct()
      val valid = recipes
        .join(nutr_ing, recipes("ID_RECIPE") === nutr_ing("RECIPE"), "left")
        .filter("RECIPE IS NOT NULL")
        .select(col("RECIPE").as("ID_RECIPE")).distinct()
      println(s"Valid: ${valid.count}")
      val invalid_nutrition = nutrition.filter(col("CALORIES") === lit(0).cast(FloatType))
        .select(col("ID_RECIPE").as("RECIPE")).distinct()
      println(s"Invalid by nutrition: ${invalid_nutrition.count()}")
      val result = valid.join(invalid_nutrition, valid("ID_RECIPE") === invalid_nutrition("RECIPE"), "left")
        .filter("RECIPE IS NULL")
        .select(col("ID_RECIPE"))
      println(s"Recipes size 2: ${result.count()}")
      result
    }

    def filterDataset(valid_recipes: DataFrame, dfs: Seq[DataFrame]): Seq[DataFrame] = {
      dfs.map(df => {
        println(s"Initial size: ${df.count()}")
        val result = df.join(valid_recipes, "ID_RECIPE")
          .select(df.columns.map(df(_)):_*)
        println(s"Final size: ${result.count()}")
        result
      })
    }

    def getAggValidUserRecipes(valid_user_recipes: DataFrame): DataFrame = {
      valid_user_recipes
        .withColumn("type",
          when(valid_user_recipes("RECIPE_TYPE").isin("recipes", "madeit", "fav"), lit("recipes").cast(StringType))
            .otherwise(lit("reviews").cast(StringType)))
        .drop("RECIPE_TYPE")
    }

    /*
    def filterUserRecipes(valid_user_recipes: DataFrame): DataFrame = {
      val recipes = valid_user_recipes.filter("type = 'recipes'")
      val reviews = valid_user_recipes.filter("type = 'reviews'")
      val invalid = recipes.join(reviews, recipes("ID_RECIPE") === reviews("ID_RECIPE") && recipes("ID_USER") === reviews("ID_USER"))
        .select(recipes("ID_RECIPE"), recipes("ID_USER"))
      valid_user_recipes.join(invalid, valid_user_recipes("ID_RECIPE") === invalid("ID_RECIPE") && valid_user_recipes("ID_USER") === invalid("ID_USER"), "left")
        .filter(invalid("ID_RECIPE").isNotNull)
    }
    */

    def getStats(user_recipes_agg: DataFrame): DataFrame = {
      user_recipes_agg
        .groupBy("ID_USER")
        .pivot("type", Seq("reviews", "recipes"))
        .count()
        .withColumn("total_recipes", col("recipes"))
        .withColumn("total_reviews", coalesce(col("reviews"), lit(0)))
        .drop("reviews", "recipes", "type")
    }

    /*EDA function*/
    def writeStats(stats: DataFrame) = {
      def filter(df: DataFrame, min: Int, max: Int, col: Column): Seq[(Int, Long)] = {
        (min to max).flatMap(i => {
          Seq((i, df.filter(col >= lit(i)).count()))
        })
      }
      val recipes = filter(stats, 30, 100, col("total_recipes"))
      val reviews = filter(stats, 10, 100, col("total_reviews"))

      val path = "./src/main/resources/output/recommendation/allrecipes"
      CSVManager.openCSVWriter(path, "recipe_stats.csv", '\t').writeAll(Seq(recipes.map({case(a,b)=>Seq(a, b)}):_*))
      CSVManager.openCSVWriter(path, "review_stats.csv", '\t').writeAll(Seq(reviews.map({case(a,b)=>Seq(a, b)}):_*))
    }

    object sampling{
      def recipesAndReviewsPercent(stats: DataFrame, user_recipes: DataFrame, configuration: Map[String, Any], minReviews: Int): (DataFrame, DataFrame) = {
        def filter(statsDF: DataFrame, df: DataFrame, ratioColumn: String): DataFrame = {
          statsDF.collect().flatMap(row => {
            val df1 = df.filter(df("ID_USER") === lit(row.getInt(0)))
            val ratio = row.getAs[Double](ratioColumn)
            if(ratio == 0.toDouble) {
              println(s"ratio is ${ratio}")
              println(row)
            }
            val df2 = df1.sample(false, ratio, System.currentTimeMillis())
            Seq(df2)
          }).reduce(_ union _)
        }
        println(s"Total users: ${stats.count()}")
        val valid_stats = stats.filter(s"total_reviews >= ${minReviews}")
        println(s"Users with more than ${minReviews} reviews: ${valid_stats.count()}")

        val trainingRatio = configuration.getOrElse("training", DEFAULT_TRAINING_SIZE)
        val testRatio = configuration.getOrElse("test", DEFAULT_TEST_SIZE)
        val df = valid_stats
          .withColumn("reviews", floor(SparkUtils.sql.min(lit(testRatio).cast(FloatType) * col("total_recipes"), col("total_reviews"))).cast(IntegerType))
          .withColumn("recipes", floor(SparkUtils.sql.min(lit(trainingRatio).cast(FloatType) * col("total_recipes") / testRatio, col("total_recipes").cast(FloatType) * trainingRatio)).cast(IntegerType))

        val recipes = user_recipes
          .filter("type = 'recipes'")
          .drop("type")
        val reviews = user_recipes
          .filter("type = 'reviews'")
          .drop("type")
        val r = df
          .withColumn("ratio_recipes", round(df("recipes").cast(DoubleType) / df("total_recipes").cast(DoubleType), 3))
          .withColumn("ratio_reviews", round(df("reviews").cast(DoubleType) / df("total_reviews").cast(DoubleType), 3))
        r.cache().count()
        recipes.cache()
        val trainingSet = filter(r, recipes, "ratio_recipes")
        recipes.unpersist()
        reviews.cache()
        val testSet = filter(r, reviews, "ratio_reviews")
        reviews.unpersist()
        println(s"Training set size: ${trainingSet.count()}")
        println(s"Test set size: ${testSet.count()}")
        r.unpersist()
        trainingSet.show()
        testSet.show()
        (trainingSet, testSet)
      }

      def recipesAndReviews(stats: DataFrame, user_recipes: DataFrame, configuration: Map[String, Any], minReviews: Int): (DataFrame, DataFrame) = {
        println(s"Total users: ${stats.count()}")
        val valid_stats = stats.filter(s"total_reviews >= ${minReviews} AND total_recipes >= total_reviews")
        println(s"Users with more than ${minReviews} reviews: ${valid_stats.count()}")

        val trainingSet = user_recipes.filter("type = 'recipes'").select("ID_RECIPE").distinct()
        val testSet = user_recipes.filter("type = 'reviews'").select("ID_RECIPE").distinct()
        println(s"Training set size: ${trainingSet.count()}")
        println(s"Test set size: ${testSet.count()}")
        //trainingSet.show()
        //testSet.show()
        (trainingSet, testSet)
      }
    }

    def computeIDF(recipes: DataFrame, ingredients: DataFrame): DataFrame = {

      def IDF(ndocuments: Long, ndocumentD: Column) : Column = {
        round(log(10, lit(ndocuments).cast(IntegerType) / ndocumentD), 4)
      }

      val nrecipes = recipes.count()
      ingredients
        .groupBy("ID_INGREDIENT")
        .count()
        .withColumn("IDF", IDF(nrecipes, col("count")))
        .drop("count")
    }


    lazy val recipes = SparkUtils.readCSV(baseInputPath, "recipes", Some(options), None)
    val nutr = SparkUtils.readCSV(baseInputPath, "nutrition", Some(options), None)
    lazy val nutrition = nutr.select(
      Seq(nutr("ID_RECIPE")) ++
        nutr.columns.filterNot(_ == "ID_RECIPE").map(col(_).cast(FloatType)):_*
    )
    lazy val ingredients = SparkUtils.readCSV(baseInputPath, "ingredients", Some(options), None)
      .withColumn("ID_INGREDIENT", col("ID_INGREDIENT").cast(IntegerType))
    lazy val user_recipes = SparkUtils.readCSV(baseInputPath, "user-recipe", Some(options), None)
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))
    lazy val reviews = SparkUtils.readCSV(baseInputPath, "reviews", Some(options), None)
    lazy val steps = SparkUtils.readCSV(baseInputPath, "steps", Some(options), None)

    /*Preprocessing*/
    val valid_recipes = getValidRecipes(recipes.select("ID_RECIPE"), ingredients, nutrition)
    val validated = filterDataset(valid_recipes, Seq(
      recipes,
      nutrition,
      ingredients,
      user_recipes,
      reviews,
      steps
    ))
    SparkUtils.writeCSV(validated(0), datasetPath, "recipes", Some(options))
    SparkUtils.writeCSV(validated(1), datasetPath, "nutrition", Some(options))
    SparkUtils.writeCSV(validated(2), datasetPath, "ingredients", Some(options))
    SparkUtils.writeCSV(validated(3), datasetPath, "user-recipe", Some(options))
    SparkUtils.writeCSV(validated(4), datasetPath, "reviews", Some(options))
    SparkUtils.writeCSV(validated(5), datasetPath, "steps", Some(options))

    val valid_user_recipes = validated(3)
    val valid_user_recipes_agg = getAggValidUserRecipes(valid_user_recipes)
    SparkUtils.writeCSV(valid_user_recipes_agg, baseOutputPath, "valid_user_recipes_agg", Some(options))

    /*Compute ingredients vector*/
    //val idf = computeIDF(validated(2))
    val idf = computeIDF(SparkUtils.readCSV(datasetPath, "recipes", Some(options), None), SparkUtils.readCSV(datasetPath, "ingredients", Some(options), None))
    SparkUtils.writeCSV(idf, baseOutputPath, "idf", Some(options))

    /*Sampling*/
    val stats = getStats(valid_user_recipes_agg)
    SparkUtils.writeCSV(stats, baseOutputPath, "stats", Some(options))

    //val idf = SparkUtils.readCSV(baseOutputPath, "idf", Some(options), None)

    val statsSchema = StructType(Seq(
      StructField("ID_USER", IntegerType),
      StructField("total_recipes", IntegerType),
      StructField("total_reviews", IntegerType)
    ))
    val userSchema = StructType(Seq(
      StructField("ID_RECIPE", IntegerType),
      StructField("ID_USER", IntegerType),
      StructField("type", StringType)
    ))

    //val stats = SparkUtils.readCSV(baseOutputPath, "stats", Some(options), Some(statsSchema))
    //val valid_user_recipes_agg = SparkUtils.readCSV(baseOutputPath, "valid_user_recipes_agg", Some(options), Some(userSchema))
    val (training, test) = sampling.recipesAndReviews(stats, valid_user_recipes_agg, Map(), 0)
    val recipesTraining = SparkUtils.readCSV(datasetPath, "recipes", Some(options), None)
    val nutritionTraining = SparkUtils.readCSV(datasetPath, "nutrition", Some(options), None)
    val ingredientsTraining = SparkUtils.readCSV(datasetPath, "ingredients", Some(options), None)
    val user_recipesTraining = SparkUtils.readCSV(datasetPath, "user-recipe", Some(options), None)
    val reviewsTraining = SparkUtils.readCSV(datasetPath, "reviews", Some(options), None)
    val stepsTraining = SparkUtils.readCSV(datasetPath, "steps", Some(options), None)
    val trainingDataset = filterDataset(training.select("ID_RECIPE"), Seq(
      recipesTraining,
      nutritionTraining,
      ingredientsTraining,
      user_recipesTraining,
      reviewsTraining,
      stepsTraining
    ))
    SparkUtils.writeCSV(trainingDataset(0), trainingPath, "recipes", Some(options))
    SparkUtils.writeCSV(trainingDataset(1), trainingPath, "nutrition", Some(options))
    SparkUtils.writeCSV(trainingDataset(2), trainingPath, "ingredients", Some(options))
    SparkUtils.writeCSV(trainingDataset(3), trainingPath, "user-recipe", Some(options))
    SparkUtils.writeCSV(trainingDataset(4), trainingPath, "reviews", Some(options))
    SparkUtils.writeCSV(trainingDataset(5), trainingPath, "steps", Some(options))

    val recipesTest = SparkUtils.readCSV(datasetPath, "recipes", Some(options), None)
    val nutritionTest = SparkUtils.readCSV(datasetPath, "nutrition", Some(options), None)
    val ingredientsTest = SparkUtils.readCSV(datasetPath, "ingredients", Some(options), None)
    val user_recipesTest = SparkUtils.readCSV(datasetPath, "user-recipe", Some(options), None)
    val reviewsTest = SparkUtils.readCSV(datasetPath, "reviews", Some(options), None)
    val stepsTest = SparkUtils.readCSV(datasetPath, "steps", Some(options), None)
    val testDataset = filterDataset(test.select("ID_RECIPE"), Seq(
      recipesTest,
      nutritionTest,
      ingredientsTest,
      user_recipesTest,
      reviewsTest,
      stepsTest
    ))
    SparkUtils.writeCSV(testDataset(0), testPath, "recipes", Some(options))
    SparkUtils.writeCSV(testDataset(1), testPath, "nutrition", Some(options))
    SparkUtils.writeCSV(testDataset(2), testPath, "ingredients", Some(options))
    SparkUtils.writeCSV(testDataset(2), testPath, "ingredients", Some(options))
    SparkUtils.writeCSV(testDataset(3).drop("RECIPE_TYPE").dropDuplicates(Seq("ID_RECIPE", "ID_USER")), testPath, "user-recipe", Some(options))
    SparkUtils.writeCSV(testDataset(4), testPath, "reviews", Some(options))
    SparkUtils.writeCSV(testDataset(5), testPath, "steps", Some(options))
  }

  def profileLearner{
    def computeNutritionSum(user_recipe: DataFrame, nutrition: DataFrame): DataFrame = {
      val nutrition_as1 = nutrition.withColumnRenamed("ID_RECIPE", "RECIPE")
      val user_nutrition = user_recipe.join(nutrition_as1, user_recipe("ID_RECIPE") === nutrition_as1("RECIPE"))
        .select(
          (Seq(
            user_recipe("ID_USER"),
            user_recipe("ID_RECIPE")
          ) ++ nutrition_as1.columns.filterNot(_ == "RECIPE").map(col(_))):_*
        )
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

    def computeNutritionAvg(user_recipe: DataFrame, nutrition: DataFrame): DataFrame = {
      val nutrition_as1 = nutrition.withColumnRenamed("ID_RECIPE", "RECIPE")
      val user_nutrition = user_recipe.join(nutrition_as1, user_recipe("ID_RECIPE") === nutrition_as1("RECIPE"))
        .select(
          (Seq(
            user_recipe("ID_USER"),
            user_recipe("ID_RECIPE")
          ) ++ nutrition_as1.columns.filterNot(_ == "RECIPE").map(col(_))):_*
        )
      val agg = user_nutrition.groupBy("ID_USER").avg(user_nutrition.columns.filterNot(Seq("ID_RECIPE", "ID_USER") contains _):_*)
      agg
        .select((agg.columns.map(name =>{
          if(name != "ID_USER")
            round(col(name), 3).as(name.substring(name.indexOf('(') + 1, name.indexOf(')')))
          else
            col(name)
        }
        )):_*)
    }

    def computeIngredients(user_recipe: DataFrame, ingredients: DataFrame, idf: DataFrame): DataFrame = {
      val ingredients_as1 = ingredients.select(
        ingredients.columns.filterNot(_ == "ID_RECIPE").map(col(_))
          :+ col("ID_RECIPE").as("RECIPE"):_*)
      val user_ingredients = user_recipe.join(ingredients_as1, user_recipe("ID_RECIPE") === ingredients_as1("RECIPE"))
      user_ingredients.groupBy("ID_USER", "ID_INGREDIENT").count().withColumnRenamed("count", "N")
        .join(idf, "ID_INGREDIENT")
        .select(
          col("ID_USER"),
          col("ID_INGREDIENT"),
          col("N"),
          round((col("N") * col("IDF")), 4).as("WEIGHTED-N").cast(FloatType)
        )
    }

    val nutrSchema = StructType(Seq(
      StructField("ID_RECIPE", StringType),
      StructField("CALCIUM", FloatType),
      StructField("CALORIES", FloatType),
      StructField("CALORIESFROMFAT", FloatType),
      StructField("CARBOHYDRATES", FloatType),
      StructField("CHOLESTEROL", FloatType),
      StructField("FAT", FloatType),
      StructField("FIBER", FloatType),
      StructField("FOLATE", FloatType),
      StructField("IRON", FloatType),
      StructField("MAGNESIUM", FloatType),
      StructField("NIACIN", FloatType),
      StructField("POTASSIUM", FloatType),
      StructField("PROTEIN", FloatType),
      StructField("SATURATEDFAT", FloatType),
      StructField("SODIUM", FloatType),
      StructField("SUGARS", FloatType),
      StructField("THIAMIN", FloatType),
      StructField("VITAMINA", FloatType),
      StructField("VITAMINB6", FloatType),
      StructField("VITAMINC", FloatType)
    ))
    val userSchema = StructType(Seq(
      StructField("RECIPE_TYPE", StringType),
      StructField("ID_RECIPE", IntegerType),
      StructField("ID_USER", IntegerType)
    ))
    val nutr = SparkUtils.readCSV(trainingPath, "nutrition", Some(options), None)
    val nutrition = nutr.select(Seq(nutr("ID_RECIPE")) ++
      nutr.columns.filterNot(_ == "ID_RECIPE").map(col(_).cast(FloatType)):_*)
    val user_recipe = SparkUtils.readCSV(trainingPath, "user-recipe", Some(options), Some(userSchema))
      .drop("RECIPE_TYPE")
    val ingredients = SparkUtils.readCSV(trainingPath, "ingredients", Some(options), None)
    val nutritionByUserAvg = computeNutritionAvg(user_recipe, nutrition)
    val nutritionByUserSum = computeNutritionSum(user_recipe, nutrition)
    val idf = SparkUtils.readCSV(baseOutputPath, "idf", Some(options), None)
    val ingredientsByUser = computeIngredients(user_recipe, ingredients, idf)
    SparkUtils.writeCSV(nutritionByUserAvg, baseOutputPath, "nutrition_profile_avg", Some(options))
    SparkUtils.writeCSV(nutritionByUserSum, baseOutputPath, "nutrition_profile_sum", Some(options))
    SparkUtils.writeCSV(ingredientsByUser, baseOutputPath, "ingredients_profile", Some(options))
  }

  def filteringComponent(threshold: Int) = {
    val nutrSchema = StructType(Seq(
      StructField("ID_RECIPE", StringType),
      StructField("CALCIUM", DoubleType),
      StructField("CALORIES", DoubleType),
      StructField("CALORIESFROMFAT", DoubleType),
      StructField("CARBOHYDRATES", DoubleType),
      StructField("CHOLESTEROL", DoubleType),
      StructField("FAT", DoubleType),
      StructField("FIBER", DoubleType),
      StructField("FOLATE", DoubleType),
      StructField("IRON", DoubleType),
      StructField("MAGNESIUM", DoubleType),
      StructField("NIACIN", DoubleType),
      StructField("POTASSIUM", DoubleType),
      StructField("PROTEIN", DoubleType),
      StructField("SATURATEDFAT", DoubleType),
      StructField("SODIUM", DoubleType),
      StructField("SUGARS", DoubleType),
      StructField("THIAMIN", DoubleType),
      StructField("VITAMINA", DoubleType),
      StructField("VITAMINB6", DoubleType),
      StructField("VITAMINC", DoubleType)
    ))
    val ingSchema = StructType(Seq(
      StructField("ID_USER", IntegerType),
      StructField("ID_INGREDIENT", IntegerType),
      StructField("N", DoubleType),
      StructField("WEIGHTED-N", DoubleType)
    ))
    val userSchema = StructType(Seq(
      StructField("ID_RECIPE", IntegerType),
      StructField("ID_USER", IntegerType)
    ))

    def nutritionSimilarity(profile: Row, nutrition: Row): Double = {
      SparkUtils.sql.cosine(profile, nutrition, nutrition.schema.fields.map(_.name).filterNot(Seq("ID_RECIPE", "ID_USER") contains _))
    }

    def ingredientsSimilarity(profile: DataFrame, ingredients: DataFrame, usingCol: String): Double = {
      def mod(df: DataFrame, column: String): Double = {
        scala.math.sqrt(df.collect().map(row => {
          val value = row.getAs[Double](column)
          value * value
        }) sum)
      }
      val df = profile.join(ingredients, "ID_INGREDIENT").select(profile.columns.map(profile(_)):_*)
      val numerator = df.collect().map(_.getAs[Double](usingCol)).sum
      val denominator = mod(profile, usingCol) * mod(ingredients.withColumn(usingCol, lit(1).cast(DoubleType)), usingCol)
      numerator / denominator
    }

    val recipes = SparkUtils.readCSV(testPath, "recipes", Some(options), None)
    val user_recipe = SparkUtils.readCSV(testPath, "user-recipe", Some(options), Some(userSchema))
    val ingredients = SparkUtils.readCSV(testPath, "ingredients", Some(options), None)
    val nutr = SparkUtils.readCSV(testPath, "nutrition", Some(options), None)
    val nutrition = nutr.select(Seq(nutr("ID_RECIPE").cast(IntegerType)) ++
      nutr.columns.filterNot(_ == "ID_RECIPE").map(col(_).cast(DoubleType)):_*)
    val reviews = SparkUtils.readCSV(testPath, "reviews", Some(options), None)
    val nagg = SparkUtils.readCSV(baseOutputPath, "nutrition_profile_avg", Some(options), None)
    val nutrition_agg = nagg.select(Seq(nagg("ID_USER").cast(IntegerType)) ++
      nagg.columns.filterNot(_ == "ID_USER").map(col(_).cast(DoubleType)):_*)
    val ingredients_agg = SparkUtils.readCSV(baseOutputPath, "ingredients_profile", Some(options), Some(ingSchema))
    val testWithRating = reviews
      .select(col("ID_RECIPE").cast(IntegerType), col("ID_AUTHOR").cast(IntegerType).as("ID_USER"), col("RATING").cast(IntegerType))

    val outputCSV = CSVManager.openCSVWriter(baseOutputPath, "similarities.csv", options("sep").charAt(0))
    outputCSV.writeRow(Seq(
      "ID_USER",
      "ID_RECIPE",
      "RATING",
      "NUT_SIMILARITY",
      "ING_N_SIMILARITY",
      "ING_WEIGHTED_SIMILARITY",
      "N_SIMILARITY",
      "WEIGHTED_SIMILARITY"))

    /*
    println(nutrition.count())
    println(recipes.count())
    println(ingredients.count())
    println(reviews.count())
    println(user_recipe.count())
    println(recipes.join(nutrition, "ID_RECIPE").count())
    println(reviews.join(nutrition, "ID_RECIPE").count())
    println(ingredients.join(nutrition, "ID_RECIPE").count())
    println(recipes.join(reviews, "ID_RECIPE").count())
    println(recipes.join(user_recipe, "ID_RECIPE").count())
    */

    val users = user_recipe.select("ID_USER").distinct().cache().collect().iterator
    var continue = true
    var recommendations = 0
    do{
      val rowUser = users.next()
      val userID = rowUser.getInt(0)
      println(s"User: ${userID}")
      val recipesDF = testWithRating.filter(s"ID_USER = ${userID}").select("ID_RECIPE", "RATING")
      val userNutrition = nutrition_agg.filter(s"ID_USER = ${userID}").head
      val userIngredients = ingredients_agg.filter(s"ID_USER = ${userID}")

      /*
      val similarities: Seq[(String, String, String, String)] = recipesDF.collect().flatMap(rowRecipe => {
        val recipeID = rowRecipe.getInt(0)
        val rating = rowRecipe.getInt(1)
        val recipeNutr = nutrition.filter(s"ID_RECIPE = '${recipeID}'").head()
        val nutrSimilarity: Float = nutritionSimilarity(userNutrition, recipeNutr)
        Map(recipeID -> nutrSimilarity)

        val recipeIng = ingredients.filter(s"ID_RECIPE = '${recipeID}'")
        val ingSimilarity: Float = ingredientsSimilarity(userIngredients, recipeIng)
        Seq((userID.toString, recipeID.toString, rating.toString, ((ingSimilarity + nutrSimilarity) / 2).formatted(".2f")))
      })
      */
      val user_recipes = recipesDF.collect().iterator
      do{
        val rowRecipe = user_recipes.next()
        val recipeID = rowRecipe.getInt(0)
        val rating = rowRecipe.getInt(1)
        println(s"Recipe: ${recipeID}")

        val recipeNutr = nutrition.filter(s"ID_RECIPE = ${recipeID}")
        val nutrSimilarity: Double = if(recipeNutr.count() == 0) 0.0f else nutritionSimilarity(userNutrition, recipeNutr.head())

        val recipeIng = ingredients.filter(s"ID_RECIPE = ${recipeID}")
        val ingSimilarity1: Double = if(recipeIng.count() == 0) 0.0f else ingredientsSimilarity(userIngredients, recipeIng, "WEIGHTED-N")
        val ingSimilarity2: Double = if(recipeIng.count() == 0) 0.0f else ingredientsSimilarity(userIngredients, recipeIng, "N")

        val similarity1:Double = (ingSimilarity1 + nutrSimilarity) / 2
        val similarity2:Double = (ingSimilarity2 + nutrSimilarity) / 2
        /*
        if(similarity.isNaN){
          println(s"Similarity is NaN: ${ingSimilarity}, ${nutrSimilarity}")
        }
        */
        outputCSV.writeRow(Seq(userID.toString, recipeID.toString, rating.toString,
          nutrSimilarity.formatted("%.4f"),
          ingSimilarity1.formatted("%.4f"),
          ingSimilarity2.formatted("%.4f"),
          similarity1.formatted("%.4f"),
          similarity2.formatted("%.4f")))
        recommendations += 1
        continue = recommendations < threshold
      } while(user_recipes.hasNext && continue)
      //outputCSV.writeAll(similarities.map(a => a.))
    } while(users.hasNext && continue)

    CSVManager.closeCSVWriter(outputCSV)
  }

  def evaluate = {

    def normalizeSimilarities(similarities: DataFrame): DataFrame = {
      def minmaxNormalize(minCurr: Column, maxCurr: Column, minNew: Column, maxNew: Column, value: Column): Column = {
        ((value - minCurr) / (maxCurr - minCurr)) * (maxNew - minNew) + minNew
      }

      def floorOrCeil(value: Column): Column = {
        val interval = ceil(value) - floor(value)
        when((interval - value).cast(DoubleType) > lit(0.5).cast(DoubleType), floor(value).cast(IntegerType))
          .otherwise(ceil(value).cast(IntegerType))
      }
      val minCurr = lit(0).cast(DoubleType)
      val maxCurr = lit(1).cast(DoubleType)
      val minNew = lit(0).cast(DoubleType)
      val maxNew = lit(5).cast(DoubleType)
      similarities
        .withColumn("SCALED_NUT_SIMILARITY", round(minmaxNormalize(minCurr, maxCurr, minNew, maxNew, col("NUT_SIMILARITY")), 4))
        .withColumn("SCALED_ING_N_SIMILARITY", round(minmaxNormalize(minCurr, maxCurr, minNew, maxNew, col("ING_N_SIMILARITY")), 4))
        .withColumn("SCALED_ING_WEIGHTED_SIMILARITY", round(minmaxNormalize(minCurr, maxCurr, minNew, maxNew, col("ING_WEIGHTED_SIMILARITY")), 4))
        .withColumn("SCALED_N_SIMILARITY", round(minmaxNormalize(minCurr, maxCurr, minNew, maxNew, col("N_SIMILARITY")), 4))
        .withColumn("SCALED_WEIGHTED_SIMILARITY", round(minmaxNormalize(minCurr, maxCurr, minNew, maxNew, col("WEIGHTED_SIMILARITY")), 4))
        .withColumn("RATING_NUTRITION", floorOrCeil(col("SCALED_NUT_SIMILARITY")))
        .withColumn("RATING_FREQ_INGREDIENT", floorOrCeil(col("SCALED_ING_N_SIMILARITY")))
        .withColumn("RATING_WEIGHTED_INGREDIENT", floorOrCeil(col("SCALED_ING_WEIGHTED_SIMILARITY")))
        .withColumn("RATING_FREQUENCY", floorOrCeil(col("SCALED_N_SIMILARITY")))
        .withColumn("RATING_WEIGHTED", floorOrCeil(col("SCALED_WEIGHTED_SIMILARITY")))
    }

    def binary(similarities: DataFrame) = {

      val threshold1 = 3
      val threshold2 = 0.5
      val binaryResults: Seq[Seq[String]] = Seq() :+
        evaluation.binaryEvaluation(similarities, "RATING", "RATING_NUTRITION", threshold1, threshold1)
        evaluation.binaryEvaluation(similarities, "RATING", "RATING_FREQ_INGREDIENT", threshold1, threshold1)
        evaluation.binaryEvaluation(similarities, "RATING", "RATING_WEIGHTED_INGREDIENT", threshold1, threshold1)
        evaluation.binaryEvaluation(similarities, "RATING", "RATING_FREQUENCY", threshold1, threshold1)
        evaluation.binaryEvaluation(similarities, "RATING", "RATING_WEIGHTED", threshold1, threshold1)
        evaluation.binaryEvaluation(similarities, "RATING", "SCALED_NUT_SIMILARITY", threshold1, threshold2)
        evaluation.binaryEvaluation(similarities, "RATING", "SCALED_ING_N_SIMILARITY", threshold1, threshold2)
        evaluation.binaryEvaluation(similarities, "RATING", "SCALED_ING_WEIGHTED_SIMILARITY", threshold1, threshold2)
        evaluation.binaryEvaluation(similarities, "RATING", "SCALED_N_SIMILARITY", threshold1, threshold2)
        evaluation.binaryEvaluation(similarities, "RATING", "SCALED_WEIGHTED_SIMILARITY", threshold1, threshold2)
      val csv = CSVManager.openCSVWriter(baseOutputPath, "binaryEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL","AREA_PR","AREA_ROC","PrecisionNOT","Precision","RecallNOT","Recall","FMeasureNOT","FMeasure"))
      csv.writeAll(binaryResults)
      CSVManager.closeCSVWriter(csv)
    }

    def regression(similarities: DataFrame) = {
      val regressionResults: Seq[(String, String, String)] = Seq(
        evaluation.regressionEvaluation(similarities, "RATING", "RATING_NUTRITION"),
        evaluation.regressionEvaluation(similarities, "RATING", "RATING_FREQ_INGREDIENT"),
        evaluation.regressionEvaluation(similarities, "RATING", "RATING_WEIGHTED_INGREDIENT"),
        evaluation.regressionEvaluation(similarities, "RATING", "RATING_FREQUENCY"),
        evaluation.regressionEvaluation(similarities, "RATING", "RATING_WEIGHTED"),
        evaluation.regressionEvaluation(similarities, "RATING", "SCALED_NUT_SIMILARITY"),
        evaluation.regressionEvaluation(similarities, "RATING", "SCALED_ING_N_SIMILARITY"),
        evaluation.regressionEvaluation(similarities, "RATING", "SCALED_ING_WEIGHTED_SIMILARITY"),
        evaluation.regressionEvaluation(similarities, "RATING", "SCALED_N_SIMILARITY"),
        evaluation.regressionEvaluation(similarities, "RATING", "SCALED_WEIGHTED_SIMILARITY"))

      val csv = CSVManager.openCSVWriter(baseOutputPath, "regressionEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL", "MSE", "MAE", "RMSE"))
      csv.writeAll(regressionResults.map(_.productIterator.toSeq))
      CSVManager.closeCSVWriter(csv)
    }

    def rankingEvaluation(similarities: DataFrame) = {
      val k_values = Seq(10, 5, 50, 100)
      val columns = Seq(
        "NUT_SIMILARITY",
        "ING_N_SIMILARITY",
        "ING_WEIGHTED_SIMILARITY",
        "N_SIMILARITY",
        "WEIGHTED_SIMILARITY")
      val seqs: Seq[Seq[Float]] = columns.map(column => {
        evaluation.precisionAtK(similarities, "ID_USER", "ID_RECIPE", "RATING", column, k_values)
      })
      val csv = CSVManager.openCSVWriter(baseOutputPath, "rankingEvaluation.csv", '|')
      csv.writeRow(k_values.map(s => s"top@$s"))
      csv.writeAll(seqs)
      CSVManager.closeCSVWriter(csv)
    }

    /*
    val sim = readCSV(baseOutputPath, "similarities", Some(options), None)
    val similarities = sim.select(
      Seq(col("ID_USER"), col("ID_RECIPE")) ++
        sim.columns.filter(!Seq("ID_USER", "ID_RECIPE").contains(_)).map(col(_).cast(DoubleType)):_*)
    val scaled_similarities = normalizeSimilarities(similarities)
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))
      .withColumn("ID_RECIPE", col("ID_RECIPE").cast(IntegerType))
    writeCSV(scaled_similarities, baseOutputPath, "scaled", Some(options))
    */

    val scaled_similarities = readCSV(baseOutputPath, "scaled", Some(options), None)
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))
      .withColumn("ID_RECIPE", col("ID_RECIPE").cast(IntegerType))

    regression(scaled_similarities)
    binary(scaled_similarities)

    val threshold1 = 3
    val threshold2 = 0.5
    val manualResults: Seq[Seq[String]] = Seq(
      evaluation.manualEvaluation(scaled_similarities, "RATING", "NORM_RATING", threshold1, threshold1).map(_.formatted("%.3f")),
      evaluation.manualEvaluation(scaled_similarities, "RATING", "predicted_rating", threshold1, threshold1).map(_.formatted("%.3f")))
    val csv = CSVManager.openCSVWriter(baseOutputPath, "manualEvaluation.csv", '|')
    csv.writeRow(Seq("LABEL", "TP", "TN", "FP", "FN", "ACC", "PPV", "NPV", "TPR", "TNR", "FPR", "FNR"))
    csv.writeAll(manualResults)
    CSVManager.closeCSVWriter(csv)
    rankingEvaluation(scaled_similarities)

    /*old method*/
    /*
    def regressionEvaluation(df: DataFrame, col1: String, col2: String) = {
      println(s"$col1 and $col2")
      val regressionMetrics = new RegressionMetrics(df
        .select(
          col(col1).cast(DoubleType),
          col(col2).cast(DoubleType))
        .rdd.map(r => (r.getDouble(0), r.getDouble(1))))

      println(s"MSE = ${regressionMetrics.meanSquaredError}")
      println(s"MAE = ${regressionMetrics.meanAbsoluteError}")
      println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")
      println()
    }

    def binaryEvaluation(df: DataFrame, col1: String, col2: String, threshold: Int) = {
      println(s"$col1 and $col2")
      val binaryMetrics = new BinaryClassificationMetrics(df
        .select(
          col(col1).cast(DoubleType),
          col(col2).cast(DoubleType))
        .rdd.map(r => (if(r.getDouble(0) < threshold) 0 else 1, if(r.getDouble(1) < threshold) 0 else 1)))

      val precision = binaryMetrics.precisionByThreshold
      precision.foreach { case (t, p) =>
        println(s"Threshold: $t, Precision: $p")
      }
    }


    val sim = readCSV(baseOutputPath, "similarities", Some(options), None)
    val similarities = sim.select(
      Seq(col("ID_USER"), col("ID_RECIPE")) ++
        sim.columns.filter(!Seq("ID_USER", "ID_RECIPE").contains(_)).map(col(_).cast(DoubleType)):_*)
    val scaled_similarities = normalizeSimilarities(similarities)
    writeCSV(scaled_similarities, baseOutputPath, "scaled", Some(options))

    val scaled_similarities = readCSV(baseOutputPath, "scaled", Some(options), None)

    regressionEvaluation(scaled_similarities, "RATING", "RATING_NUTRITION")
    regressionEvaluation(scaled_similarities, "RATING", "RATING_FREQ_INGREDIENT")
    regressionEvaluation(scaled_similarities, "RATING", "RATING_WEIGHTED_INGREDIENT")
    regressionEvaluation(scaled_similarities, "RATING", "RATING_FREQUENCY")
    regressionEvaluation(scaled_similarities, "RATING", "RATING_WEIGHTED")

    val threshold = 3
    binaryEvaluation(scaled_similarities, "RATING", "RATING_NUTRITION", threshold)
    binaryEvaluation(scaled_similarities, "RATING", "RATING_FREQ_INGREDIENT", threshold)
    binaryEvaluation(scaled_similarities, "RATING", "RATING_WEIGHTED_INGREDIENT", threshold)
    binaryEvaluation(scaled_similarities, "RATING", "RATING_FREQUENCY", threshold)
    binaryEvaluation(scaled_similarities, "RATING", "RATING_WEIGHTED", threshold)
     */
  }

}
