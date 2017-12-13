package es.uam.eps.tfm.fmendezlopez.allrecipes

import java.io.File

import com.github.tototoshi.csv.CSVWriter
import es.uam.eps.tfm.fmendezlopez.utils.{CSVManager, Logging}
import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

/**
  * Created by franm on 19/11/2017.
  */
object PropagationRecommender extends Logging{

  private var spark : SparkSession = _
  private lazy val DEFAULT_TRAINING_SIZE = 0.7f
  private lazy val DEFAULT_TEST_SIZE = 0.3f

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )
  val baseOutputPath = "./src/main/resources/output/recommendation/allrecipes/propagation"
  val baseInputPath = "./src/main/resources/output/recommendation/allrecipes/preprocessing"

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

  def contentAnalyzer = {
    object sampling{
      def percentSampling(user_recipe: DataFrame, minReviews: Int) = {
        val user_reviews = user_recipe.filter("RECIPE_TYPE = 'review'")
        val grouped = user_reviews.groupBy("ID_USER").count()
        grouped.cache().count()
        val total_users = grouped.count
        logger.info(s"Total users: ${total_users}")
        val valid_users = grouped.filter(s"count >= $minReviews").sample(false, 0.1, System.currentTimeMillis())
        logger.info(s"Discarded users: ${total_users - valid_users.count}")
        valid_users.cache()
        val valid_user_reviews = user_reviews.join(valid_users, "ID_USER").select(user_reviews.columns.map(user_reviews(_)):_*)
        valid_user_reviews.cache().count()
        var i = 0
        valid_users.collect().flatMap(row => {
          val userID = row.getInt(0)
          val this_user_reviews = valid_user_reviews.filter(s"ID_USER = ${userID}").select(col("ID_USER").as("USER"), col("ID_RECIPE").as("RECIPE"))
          val ratio: Double = DEFAULT_TRAINING_SIZE
          val training: DataFrame = this_user_reviews
            .sample(false, ratio, System.currentTimeMillis())
            .select(col("USER").as("ID_USER"), col("RECIPE").as("ID_RECIPE"))
          val test: DataFrame = this_user_reviews
            .join(training, this_user_reviews("RECIPE") === training("ID_RECIPE"), "left")
            .filter("ID_RECIPE IS NULL")
            .select(col("USER").as("ID_USER"), col("RECIPE").as("ID_RECIPE"))

          logger.debug(s"Training size: ${training.count()}")
          logger.debug(s"Test size: ${test.count()}")
          logger.debug(s"Total size: ${this_user_reviews.count()}")
          i += 1
          logger.info(s"Processed $i users")
          Seq((training, test))
        }).reduce((a, b) => (a._1 union b._1, a._2 union b._2))
      }
    }

    val user_recipes = readCSV(baseInputPath, "user-recipe", Some(options), None)
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))

    val (trainingSet, testSet) = sampling.percentSampling(user_recipes, 100)

    val reviews = readCSV(baseInputPath, "reviews", Some(options), None)
      .withColumnRenamed("ID_AUTHOR", "ID_USER")

    /*
    val training = reviews
      .join(trainingSet, reviews("ID_USER") === trainingSet("USER") && reviews("ID_RECIPE") === trainingSet("RECIPE"))
      .select(reviews.columns.map(reviews(_)):_*)

    val test = reviews
      .join(testSet, reviews("ID_USER") === testSet("USER") && reviews("ID_RECIPE") === testSet("RECIPE"))
      .select(reviews.columns.map(reviews(_)):_*)
      */
    val training = reviews
      .join(trainingSet, Seq("ID_USER", "ID_RECIPE"))
      .select("ID_USER", "ID_RECIPE", "RATING")

    val test = reviews
      .join(testSet, Seq("ID_USER", "ID_RECIPE"))
      .select("ID_USER", "ID_RECIPE", "RATING")

    writeCSV(training, baseOutputPath, "training", Some(options))
    writeCSV(test, baseOutputPath, "test", Some(options))
  }

  def profileLearner = {
    val training = readCSV(baseOutputPath, "training", Some(options), None)
      .select(col("ID_USER"), col("ID_RECIPE"), col("RATING").cast(IntegerType))
    val ingredients = readCSV(baseInputPath, "ingredients", Some(options), None)

    val ingredients_rated = training
      .join(ingredients, "ID_RECIPE")
      .select(
        training.columns.map(training(_))
          :+ col("ID_INGREDIENT")
        :_*)

    val propagated = ingredients_rated.groupBy("ID_USER", "ID_INGREDIENT").avg("RATING").withColumnRenamed("avg(RATING)", "INGR_RATING")
    writeCSV(propagated, baseOutputPath, "propagated", Some(options))
  }

  def filteringComponent = {
    val test = readCSV(baseOutputPath, "test", Some(options), None)
    val ingredients = readCSV(baseInputPath, "ingredients", Some(options), None)
    val propagated = readCSV(baseOutputPath, "propagated", Some(options), None)
      .select(col("ID_USER"), col("ID_INGREDIENT"), col("INGR_RATING").cast(DoubleType))

    val test_ingredients = test
      .join(ingredients, "ID_RECIPE")
      .select(
        test.columns.map(test(_))
        :+ col("ID_INGREDIENT")
        :_*
      )
    val test_propagated = test_ingredients
      .join(propagated, Seq("ID_USER", "ID_INGREDIENT"))
      .groupBy("ID_USER", "ID_RECIPE")
      .avg("INGR_RATING")
      .withColumnRenamed("avg(INGR_RATING)", "predicted_rating")

    val prediction = test
      .join(test_propagated, Seq("ID_USER", "ID_RECIPE"))
      .select(
        test.columns.map(test(_))
        :+ col("predicted_rating")
        :_*
      )

    writeCSV(prediction, baseOutputPath, "prediction", Some(options))
  }

  def evaluate = {

    def normalizeSimilarities(similarities: DataFrame): DataFrame = {

      def floorOrCeil(value: Column): Column = {
        val interval = ceil(value) - floor(value)
        when((interval - value).cast(DoubleType) > lit(0.5).cast(DoubleType), floor(value).cast(IntegerType))
          .otherwise(ceil(value).cast(IntegerType))
      }

      similarities
        .withColumn("NORM_RATING", floorOrCeil(col("predicted_rating")))
    }

    def rankingEvaluation(similarities: DataFrame) = {
      val k_values = Seq(10, 5, 50, 100)
      val columns = Seq(
        "NORM_RATING",
        "predicted_rating")
      val seqs: Seq[Seq[Float]] = columns.map(column => {
        evaluation.precisionAtK(similarities, "ID_USER", "ID_RECIPE", "RATING", column, k_values)
      })
      val csv = CSVManager.openCSVWriter(baseOutputPath, "rankingEvaluation.csv", '|')
      csv.writeRow(k_values.map(s => s"top@$s"))
      csv.writeAll(seqs)
      CSVManager.closeCSVWriter(csv)
    }

    val pred1 = readCSV(baseOutputPath, "prediction", Some(options), None)
    val pred2 = pred1.select(
      Seq(col("ID_USER"), col("ID_RECIPE")) ++
        pred1.columns.filter(!Seq("ID_USER", "ID_RECIPE").contains(_)).map(col(_).cast(DoubleType)):_*)
    val prediction = normalizeSimilarities(pred2)
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))
      .withColumn("ID_RECIPE", col("ID_RECIPE").cast(IntegerType))
    writeCSV(prediction, baseOutputPath, "scaled", Some(options))

    /*
    val prediction = normalizeSimilarities(readCSV(baseOutputPath, "prediction", Some(options), None))
      .withColumn("ID_USER", col("ID_USER").cast(IntegerType))
      .withColumn("ID_RECIPE", col("ID_RECIPE").cast(IntegerType))
      */
    var csv: CSVWriter = null

    val regressionResults: Seq[(String, String, String)] = Seq(
      evaluation.regressionEvaluation(prediction, "RATING", "NORM_RATING"),
      evaluation.regressionEvaluation(prediction, "RATING", "predicted_rating"))

    csv = CSVManager.openCSVWriter(baseOutputPath, "regressionEvaluation.csv", '|')
    csv.writeRow(Seq("LABEL","MSE","MAE"))
    csv.writeAll(regressionResults.map(_.productIterator.toSeq))
    CSVManager.closeCSVWriter(csv)

    val threshold1 = 3
    val threshold2 = 0.5
    val binaryResults: Seq[Seq[String]] = Seq(
      evaluation.binaryEvaluation(prediction, "RATING", "NORM_RATING", threshold1, threshold1),
      evaluation.binaryEvaluation(prediction, "RATING", "predicted_rating", threshold1, threshold1))
    csv = CSVManager.openCSVWriter(baseOutputPath, "binaryEvaluation.csv", '|')
    csv.writeRow(Seq("LABEL","AREA_PR","AREA_ROC","PrecisionNOT","Precision","RecallNOT","Recall","FMeasureNOT","FMeasure"))
    csv.writeAll(binaryResults)
    CSVManager.closeCSVWriter(csv)

    val manualResults: Seq[Seq[String]] = Seq(
      Seq("NORM_RATING") ++ evaluation.manualEvaluation(prediction, "RATING", "NORM_RATING", threshold1, threshold1).map(_.formatted("%.3f")),
      Seq("predicted_rating") ++ evaluation.manualEvaluation(prediction, "RATING", "predicted_rating", threshold1, threshold1).map(_.formatted("%.3f")))
    csv = CSVManager.openCSVWriter(baseOutputPath, "manualEvaluation.csv", '|')
    csv.writeRow(Seq("LABEL", "TP", "TN", "FP", "FN", "ACC", "PPV", "NPV", "TPR", "TNR", "FPR", "FNR"))
    csv.writeAll(manualResults)
    CSVManager.closeCSVWriter(csv)

    rankingEvaluation(prediction)
  }
}
