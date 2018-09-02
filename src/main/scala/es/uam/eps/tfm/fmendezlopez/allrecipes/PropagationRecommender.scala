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
  val baseInputPath = "./src/main/resources/input/upgraded_dataset"

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.memory", "3g")
      .appName("Allrecipes Propagation Recommender")
      .getOrCreate()

    contentAnalyzer
    profileLearner
    filteringComponent(1000)
    evaluate
  }

  def contentAnalyzer = {
    object sampling{
      def percentSampling(user_recipe: DataFrame, minReviews: Int, maxUsers: Int): (DataFrame, DataFrame) = {
        val user_reviews = user_recipe.filter("RECIPE_TYPE = 'review'")//.cache()
        val grouped = user_reviews.groupBy("USER_ID").count()
        val total_users = grouped.count
        logger.info(s"Total users: ${total_users}")
        val valid_users = grouped.filter(s"count >= $minReviews")//.sample(false, 0.1, System.currentTimeMillis())
        logger.info(s"Discarded users: ${total_users - valid_users.count}")
        val valid_user_reviews = user_reviews
          .join(valid_users, "USER_ID")
          .select(user_reviews("USER_ID").as("USER"), user_reviews("RECIPE_ID").as("RECIPE"))
          .cache()
        //user_reviews.unpersist()
        var i = 0
        val ret = valid_users.collect().take(maxUsers).flatMap(row => {
          val userID = row.getInt(0)
          val this_user_reviews = valid_user_reviews
            .filter(s"USER_ID = ${userID}")
            .select("USER", "RECIPE")
          val ratio: Double = DEFAULT_TRAINING_SIZE
          val training: DataFrame = this_user_reviews
            .sample(false, ratio, System.currentTimeMillis())
            .select(col("USER").as("USER_ID"), col("RECIPE").as("RECIPE_ID"))

          logger.info(s"Training size: ${training.count()}")
          logger.info(s"Total size: ${this_user_reviews.count()}")
          i += 1
          logger.info(s"Processed $i users")
          Seq((training))
        }).reduce((a, b) => (a union b))
        /*
        val ret = valid_users.collect().take(maxUsers).flatMap(row => {
          val userID = row.getInt(0)
          val this_user_reviews = valid_user_reviews.filter(s"USER_ID = ${userID}").select(col("USER_ID").as("USER"), col("RECIPE_ID").as("RECIPE"))
          val ratio: Double = DEFAULT_TRAINING_SIZE
          val training: DataFrame = this_user_reviews
            .sample(false, ratio, System.currentTimeMillis())
            .select(col("USER").as("USER_ID"), col("RECIPE").as("RECIPE_ID"))
          val test: DataFrame = this_user_reviews
            .join(training, this_user_reviews("RECIPE") === training("RECIPE_ID"), "left")
            .filter("RECIPE_ID IS NULL")
            .select(col("USER").as("USER_ID"), col("RECIPE").as("RECIPE_ID"))

          logger.info(s"Training size: ${training.count()}")
          logger.info(s"Test size: ${test.count()}")
          logger.info(s"Total size: ${this_user_reviews.count()}")
          i += 1
          logger.info(s"Processed $i users")
          Seq((training, test))
        }).reduce((a, b) => (a._1 union b._1, a._2 union b._2))
        */
        (ret, valid_user_reviews)
      }
    }

    val user_recipes = readCSV(baseInputPath, "user-recipe", Some(options), None)
      .withColumn("USER_ID", col("USER_ID").cast(IntegerType))

    val (trainingSet, valid_user_reviews) = sampling.percentSampling(user_recipes, 10, 200)

    val reviews = readCSV(baseInputPath, "reviews", Some(options), None)
      .withColumnRenamed("AUTHOR_ID", "USER_ID")

    val aux1 = valid_user_reviews
      .join(trainingSet, valid_user_reviews("USER") === trainingSet("USER_ID"))
      .select(valid_user_reviews("USER").as("USER_ID"), valid_user_reviews("RECIPE").as("RECIPE_ID"))
      .dropDuplicates()

    val testSet = aux1.except(trainingSet)

    val training = reviews
      .join(trainingSet, Seq("USER_ID", "RECIPE_ID"))
      .select("USER_ID", "RECIPE_ID", "RATING")

    val test = reviews
      .join(testSet, Seq("USER_ID", "RECIPE_ID"))
      .select("USER_ID", "RECIPE_ID", "RATING")

    //writeCSV(training, baseOutputPath, "training", Some(options))
    //writeCSV(test, baseOutputPath, "test", Some(options))
    training.write.options(options).parquet(s"${baseOutputPath}${File.separator}training")
    test.write.options(options).parquet(s"${baseOutputPath}${File.separator}test")
    valid_user_reviews.unpersist()
  }

  def profileLearner = {
    val training = spark.read.options(options).load(s"${baseOutputPath}${File.separator}training")
      .select(col("USER_ID"), col("RECIPE_ID"), col("RATING").cast(IntegerType))
    val ingredients = readCSV(baseInputPath, "ingredients", Some(options), None)

    val ingredients_rated = training
      .join(ingredients, "RECIPE_ID")
      .select(
        training.columns.map(training(_))
          :+ col("ID_INGREDIENT")
        :_*)

    val propagated = ingredients_rated.groupBy("USER_ID", "ID_INGREDIENT").avg("RATING").withColumnRenamed("avg(RATING)", "INGR_RATING")
    writeCSV(propagated, baseOutputPath, "propagated", Some(options))
  }

  def filteringComponent(nRatings: Int = 0) = {
    val testSet = spark.read.options(options).load(s"${baseOutputPath}${File.separator}test")
    val count = testSet.count()
    val nrows = nRatings.toLong.min(count)
    val ratio: Float = nrows.toFloat / count.toFloat
    val test = testSet.sample(false, ratio, System.currentTimeMillis())
    val ingredients = readCSV(baseInputPath, "ingredients", Some(options), None)
    val propagated = readCSV(baseOutputPath, "propagated", Some(options), None)
      .select(col("USER_ID"), col("ID_INGREDIENT"), col("INGR_RATING").cast(DoubleType))

    val test_ingredients = test
      .join(ingredients, "RECIPE_ID")
      .select(
        test.columns.map(test(_))
        :+ col("ID_INGREDIENT")
        :_*
      )
    val test_propagated = test_ingredients
      .join(propagated, Seq("USER_ID", "ID_INGREDIENT"))
      .groupBy("USER_ID", "RECIPE_ID")
      .avg("INGR_RATING")
      .withColumnRenamed("avg(INGR_RATING)", "predicted_rating")

    val prediction = test
      .join(test_propagated, Seq("USER_ID", "RECIPE_ID"))
      .select(
        test.columns.map(test(_))
        :+ col("predicted_rating")
        :+ sql.floorOrCeil(col("predicted_rating")).as("propR")
        :_*
      )

    writeCSV(prediction, baseOutputPath, "prediction", Some(options))
  }

  def evaluate = {

    def binary(similarities: DataFrame) = {
      val threshold1 = 3
      val threshold2 = 0.5
      val binaryResults: Seq[Seq[String]] = Seq(
        evaluation.binaryEvaluation(similarities, "RATING", "propR", threshold1, threshold1))
      val csv = CSVManager.openCSVWriter(baseOutputPath, "binaryEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL","AREA_PR","AREA_ROC","PrecisionNOT","Precision","RecallNOT","Recall","FMeasureNOT","FMeasure"))
      csv.writeAll(binaryResults)
      CSVManager.closeCSVWriter(csv)
    }

    def regression(similarities: DataFrame) = {
      val regressionResults: Seq[(String, String, String, String)] = Seq(
        evaluation.regressionEvaluation(similarities, "RATING", "propR"))

      val csv = CSVManager.openCSVWriter(baseOutputPath, "regressionEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL", "MSE", "MAE", "RMSE"))
      csv.writeAll(regressionResults.map(_.productIterator.toSeq))
      CSVManager.closeCSVWriter(csv)
    }

    def rankingEvaluation(similarities: DataFrame) = {
      val k_values = Seq(5, 10, 30)
      val columns = Seq("propR")
      val thresholdRelevant = 3
      val seqs: Seq[Seq[String]] = columns.map(column => {
        evaluation.precisionAtK(similarities, "USER_ID", "RECIPE_ID", "RATING", column, k_values, thresholdRelevant)
      })
      val csv = CSVManager.openCSVWriter(baseOutputPath, "rankingEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL") ++ k_values.map(k => s"P@$k"))
      csv.writeAll(seqs)
      CSVManager.closeCSVWriter(csv)
    }

    def manualEvaluation(similarities: DataFrame) = {
      val threshold1 = 3
      val threshold2 = 0.5
      val manualResults: Seq[Seq[String]] = Seq(
        evaluation.manualEvaluation(similarities, "RATING", "propR", threshold1, threshold1)
      )
      val csv = CSVManager.openCSVWriter(baseOutputPath, "manualEvaluation.csv", '|')
      csv.writeRow(Seq("LABEL", "True Positives", "True Negatives", "False Positives", "False Negatives",
        "Accuracy", "Precision", "Negative Predictive Value", "Recall", "Specificity", "False Positive Rate",
        "False Negative Rate", "ROC"))
      csv.writeAll(manualResults)
      CSVManager.closeCSVWriter(csv)
    }

    val pred1 = readCSV(baseOutputPath, "prediction", Some(options), None)
    val pred2 = pred1.select(
      col("USER_ID").cast(IntegerType),
      col("RECIPE_ID").cast(IntegerType),
      col("RATING").cast(IntegerType),
      col("propR").cast(IntegerType))

    binary(pred2)
    regression(pred2)
    rankingEvaluation(pred2)
    manualEvaluation(pred2)
  }

}
