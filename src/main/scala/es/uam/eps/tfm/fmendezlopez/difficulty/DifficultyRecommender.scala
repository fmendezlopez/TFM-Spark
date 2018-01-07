package es.uam.eps.tfm.fmendezlopez.difficulty

import java.io.File

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
  * Created by franm on 18/12/2017.
  */
object DifficultyRecommender {
  private var spark : SparkSession = _

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )
  val baseOutputPath = "./src/main/resources/output/recommendation/allrecipes/difficulty"
  val baseInputPath = "./src/main/resources/input/recommendation/allrecipes/difficulty"

  def main(args: Array[String]): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DifficultyRecommender")
      .getOrCreate()
    contentAnalyzer
  }

  def contentAnalyzer = {
    val commonSchema = StructType(Seq(
      StructField("COOK_TIME", IntegerType),
      StructField("#INGREDIENTS", IntegerType),
      StructField("#STEPS", IntegerType),
      StructField("DIFFICULTY", StringType)
    ))

    def buildDataset: DataFrame = {

      val chowhound = readCSV(s"${baseInputPath}${File.separator}chowhound", "recipes.csv", Some(options), None)
        .select(commonSchema.fields.map(field => col(field.name).cast(field.dataType)): _*)
        .filter("COOK_TIME > 0")
      val recipekey = readCSV(s"${baseInputPath}${File.separator}recipekey", "recipes.csv", Some(options), None)
        .select(commonSchema.fields.map(field => col(field.name).cast(field.dataType)): _*)
        .filter("COOK_TIME > 0")
      val cookipedia = readCSV(s"${baseInputPath}${File.separator}cookipedia", "recipes.csv", Some(options), None)
        .select(commonSchema.fields.map(field => col(field.name).cast(field.dataType)): _*)
        .filter("COOK_TIME > 0")
      val saveur = readCSV(s"${baseInputPath}${File.separator}saveur", "recipes.csv", Some(options), None)
        .select(commonSchema.fields.map(field => col(field.name).cast(field.dataType)): _*)
        .filter("COOK_TIME > 0")

      chowhound.union(cookipedia).union(saveur).union(recipekey)
    }

    def learnModel(data: DataFrame) = {
      val labelIndexer = new StringIndexer()
        .setInputCol("DIFFICULTY")
        .setOutputCol("indexedLabel")
        .fit(data)
      val vectorAssembler = new VectorAssembler()
        .setInputCols(Array(
          "COOK_TIME",
          "#INGREDIENTS",
          "#STEPS"
        ))
        .setOutputCol("features")
      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      /*
      val Row(coeff1: Matrix) = Correlation.corr(vectorAssembler.transform(data), "features").head
      println("Pearson correlation matrix:\n" + coeff1.toString)
      */


      // Train a DecisionTree model.
      val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")
        .setMaxDepth(10)
        .setMinInstancesPerNode(20)

      val nv = new NaiveBayes()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")

      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)

      // Chain indexers and tree in a Pipeline.
      val pipeline1 = new Pipeline()
        .setStages(Array(labelIndexer, vectorAssembler, dt, labelConverter))

      val pipeline2 = new Pipeline()
        .setStages(Array(labelIndexer, vectorAssembler, nv, labelConverter))

      // Train model. This also runs the indexers.
      val model1 = pipeline1.fit(trainingData)

      // Make predictions.
      val predictions1 = model1.transform(testData)

      // Select example rows to display.
      predictions1.select("predictedLabel", "DIFFICULTY", "features").show(5)
      // Select (prediction, true label) and compute test error.
      val evaluator1 = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator1.evaluate(predictions1)
      println("Test Error = " + (1.0 - accuracy))

      val treeModel = model1.stages(2).asInstanceOf[DecisionTreeClassificationModel]
      println("Learned classification tree model:\n" + treeModel.toDebugString)

      // Train model. This also runs the indexers.
      val model2 = pipeline2.fit(trainingData)

      // Make predictions.
      val predictions2 = model2.transform(testData)

      // Select example rows to display.
      predictions2.select("predictedLabel", "DIFFICULTY", "features").show(5)
      // Select (prediction, true label) and compute test error.
      val evaluator2 = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy2 = evaluator2.evaluate(predictions2)
      println("Test Error = " + (1.0 - accuracy2))
    }

    /*
    val union = buildDataset
    writeCSV(union, baseOutputPath, "union", Some(options))
    */
    val union = readCSV(baseOutputPath, "union", Some(options), None)
      .select(commonSchema.fields.map(field => col(field.name).cast(field.dataType)): _*)
    println(union.stat.corr("#STEPS", "#INGREDIENTS"))
    println(union.stat.corr("#STEPS", "COOK_TIME"))
    println(union.stat.corr("COOK_TIME", "#INGREDIENTS"))
    println(union.stat.cov("#STEPS", "#INGREDIENTS"))
    println(union.stat.cov("#STEPS", "COOK_TIME"))
    println(union.stat.cov("COOK_TIME", "#INGREDIENTS"))  
    //learnModel(union)
  }
}
