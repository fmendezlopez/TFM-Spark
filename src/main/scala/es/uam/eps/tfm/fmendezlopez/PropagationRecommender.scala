package es.uam.eps.tfm.fmendezlopez

import org.apache.spark.sql.SparkSession

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
  val baseOutputPath = "./src/main/resources/output/recommendation/allrecipes"
  val baseInputPath = "./src/main/resources/input/recommendation/allrecipes"

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Allrecipes Recommender")
      .getOrCreate()
  }


}
