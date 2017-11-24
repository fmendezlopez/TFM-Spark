package es.uam.eps.tfm.fmendezlopez

import java.io.File
import java.util.regex.Pattern

import com.github.tototoshi.csv.CSVReader
import edu.stanford.nlp.simple.Sentence
import es.uam.eps.tfm.fmendezlopez.utils.{CSVManager, Logging, SparkUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.convert._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by Francisco on 08/05/2017.
  * Scala functionalities for ingredients file preprocessing
  */
object IngredientPreprocessing extends Logging{

  private var spark : SparkSession = _

  def main(args: Array[String]): Unit = {

    spark = SparkSession
      .builder()
      .appName("TFM-Spark")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("ID_RECIPE", IntegerType),
      StructField("ID_INGREDIENT", IntegerType),
      StructField("TEXT_VALUE", StringType),
      StructField("AMOUNT", DoubleType)
    ))

    val inputDir = "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\users"
    val outputDir = "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\output\\ing_prep"
    getStats(inputDir, schema)
    /*
    execution example
    val res = step1("", schema1, Some(df2), None, 4)
    val res1 = step2("", schema1, Some(res), None)
    val res2 = step3("", schema1, Some(res), None)
    step4()
     */
  }

  def getStats(inputDir : String, schema : StructType) = {
    val options : Map[String, String] = Map(
      "sep" -> "|",
      "encoding" -> "UTF-8",
      "quote" -> ""
    )
  }


  /**
    * Filter top N most frequent ingredients
    * @param inputName name of ingredients input file
    * @param schema struct of file (must have at least ID_RECIPE|ID_INGREDIENT)
    * @param inputDF input dataframe (if defined, no input file will be read)
    * @param outputName name of output file
    * @param threshold N parameter to filter top N ingredients
    * @return result dataframe
    */
  def step1(inputName : String, schema : StructType, inputDF : Option[DataFrame], outputName : Option[String], threshold : Int) : DataFrame = {

    val df: DataFrame = if(inputDF.isDefined) inputDF.get else {spark.read
      .option("sep", "|")
      .format("csv")
      .schema(schema)
      .option("header", true)
      .csv(inputName)
      .filter(col("ID_INGREDIENT") =!= lit(0))}

    val dfD = df.dropDuplicates("ID_RECIPE", "ID_INGREDIENT")

    println(s"#recipes before: ${getNRecipes(dfD)}")
    println(s"#ingredients before: ${getNIngredients(dfD)}")

    val dfFiltered = filterGraph(dfD, threshold)
    println(s"#recipes after: ${getNRecipes(dfFiltered)}")

    if(outputName.isDefined) {
      dfFiltered.coalesce(1).write
        .option("sep", "\t")
        .option("header", true)
        .format("csv")
        .save(outputName.get)
    }
    dfFiltered
  }

  /**
    * Complete undirected graph generation
    * @param inputName name of ingredients input file
    * @param schema struct of file (must have at least ID_RECIPE|ID_INGREDIENT)
    * @param inputDF input dataframe (if defined, no input file will be read)
    * @param outputName name of output file (if not defined, no output file will be written)
    * @return result dataframe
    */
  def step2(inputName : String, schema : StructType, inputDF : Option[DataFrame], outputName : Option[String]) : DataFrame = {

    val df: DataFrame = if(inputDF.isDefined) inputDF.get else {spark.read
      .option("sep", "|")
      .format("csv")
      .schema(schema)
      .option("header", true)
      .csv(inputName)
      .filter(col("ID_INGREDIENT") =!= lit(0))}

    val graph = buildUndirectedGraph(df)
    if(outputName.isDefined){
      graph.coalesce(1).write
        .option("sep", "\t")
        .format("csv")
        .save(outputName.get)
    }
    graph
  }

  /*

   */
  /**
    * PMI undirected graph generation
    * @param inputName name of ingredients input file
    * @param schema struct of file (must have at least ID_RECIPE|ID_INGREDIENT)
    * @param inputDF input dataframe (if defined, no input file will be read)
    * @param outputName name of output file (if not defined, no output file will be written)
    * @return result dataframe
    */
  def step3(inputName : String, schema : StructType, inputDF : Option[DataFrame], outputName : Option[String]) : DataFrame = {

    val df: DataFrame = if(inputDF.isDefined) inputDF.get else {spark.read
      .option("sep", "\t")
      .format("csv")
      .schema(schema)
      .option("header", true)
      .csv(inputName)
      .filter(col("ID_INGREDIENT") =!= lit(0))}

    val graph = buildPMIGraph(df)

    if(outputName.isDefined){
      graph.coalesce(1).write
        .option("sep", "\t")
        .format("csv")
        .save(outputName.get)
    }
    graph
  }

  /**
    * Heuristic to determine ingredients names
    */
  def step4() : Unit = {
    val inputName = "C:\\Users\\Francisco\\IdeaProjects\\WebMining\\src\\main\\resources\\output\\new\\ingredients_top1000.csv"
    val outputPath = "C:\\Users\\Francisco\\IdeaProjects\\WebMining\\src\\main\\resources\\output\\new"
    val outputName = "ingredients_top1000_newNames.csv"
    val inputCSV = CSVManager.openCSVReader(new File(inputName), '|')
    inputCSV.readNext()
    val outputCSV = CSVManager.openCSVWriter(outputPath, outputName, '|')
    val result = extractIngredientWords(inputCSV)
    outputCSV.writeRow(Seq("ID_INGREDIENT", "NAME"))
    result.foreach({case(key, value) => outputCSV.writeRow(Seq(key, value.mkString(" ")))})
    CSVManager.closeCSVReader(inputCSV)
    CSVManager.closeCSVWriter(outputCSV)
  }

  /**
    * Determines ingredients names from user descriptions
    * @param input CSVReader with description of ingredients (must have at least ID_RECIPE|ID_INGREDIENT|TEXT )
    * @return map with (ID_INGREDIENT -> [words]) mapping
    */
  def extractIngredientWords(input : CSVReader) : Map[Int, Seq[String]] = {
    val pattern1 = Pattern.compile("[a-zA-Z-]+")
    val pattern2 = Pattern.compile("[,.:;()!/\\%[0-9]]")
    val invalids = Seq("tablespoon", "tablespoons", "teaspoon", "teaspoons", "cup", "cups", "ounce", "ounces", "inch", "inches")
    val linesPerIngredient = 15
    val limit = 0.5
    var result : Map[Int, Seq[String]] = Map()
    var continue = true
    var auxLine : Option[Seq[String]] = None
    while(continue){
      var i = 0
      var wordCount : mutable.Map[String, (Int, Int)] = mutable.Map()
      val first : Seq[String] = auxLine.getOrElse(input.readNext().getOrElse(Seq()))
      if(first.isEmpty)
        continue = false
      else{
        i += 1
        val currID = first(1).toInt
        val words = getMainWords(first(2), pattern1, pattern2, invalids)
        words.foreach(word => wordCount += (word -> (1, words.indexOf(word))))
        var stop = false
        while(!stop){
          val line = input.readNext().getOrElse(Seq())
          if(line.isEmpty){
            result += (currID.toInt -> words)
            stop = true
            continue = false
          }
          else{
            val ingID = line(1).toInt
            if(currID == ingID){
              i += 1
              val nextWords = getMainWords(line(2), pattern1, pattern2, invalids)

              var intersection : Set[String] = Set()
              intersection ++= words.toSet ++ nextWords.toSet
              intersection.foreach(word => {
                if(wordCount.isDefinedAt(word)) {
                  val value = wordCount(word)
                  wordCount.replace(word, (value._1 + 1, value._2))
                }
                else
                  wordCount += (word -> (1, 0))
              })
              if(i == linesPerIngredient){
                val threshold = i * limit
                var newWords : Seq[String] = Seq()
                wordCount.foreach({case(key, value) =>
                  if(value._1 >= math.floor(threshold))
                    newWords = newWords :+ key
                })
                val finalWords = newWords.sortWith((a, b) => {
                  wordCount(a)._2 < wordCount(b)._2
                })
                stop = true
                result += (currID -> finalWords)
                var terminate = false
                while(!terminate){
                  auxLine = input.readNext()
                  if(auxLine.isEmpty){
                    continue = false
                  }
                  else {
                    val ingID = auxLine.get(1).toInt
                    terminate = currID != ingID
                  }
                }
              }
            }
            else{
              val threshold = i * limit
              var newWords : Seq[String] = Seq()
              wordCount.foreach({case(key, value) =>
                if(value._1 >= math.floor(threshold))
                  newWords = newWords :+ key
              })
              val finalWords = newWords.sortWith((a, b) => {
                wordCount(a)._2 < wordCount(b)._2
              })
              stop = true
              result += (currID -> finalWords)
              auxLine = Some(line)
              stop = true
            }
          }
        }
      }
    }
    result
  }

  /**
    * Extracts the main words of a ingredient description
    * @param description description to parse
    * @param pattern1 regex to filter words
    * @param pattern2 regex to replace inside words
    * @param invalids list of invalid words
    * @return list of main words
    */
  def getMainWords(description : String, pattern1 : Pattern, pattern2 : Pattern, invalids : Seq[String]) : Seq[String] = {
    val desc = pattern2.matcher(description).replaceAll(" ")
    val sentence = new Sentence(desc)
    val words = sentence.words()
    var wordsMap : Map[String, (Int, String)] = Map()
    var index = 0
    words.asScala.foreach(word =>{
      wordsMap += (word -> (index, sentence.lemma(index)))
      index += 1
    })
    words
      .filter(elem => pattern1.matcher(elem).matches())
      .filter(elem => !invalids.contains(elem))
      .filter(elem => elem.length > 2)
      .map(word =>{
        val lemma = wordsMap(word)._2
        lemma
      })
  }

  /**
    * Gets number of ingredients in a dataframe
    * @param df input dataframe (with column ID_INGREDIENT)
    * @return number of ingredients
    */
  def getNIngredients(df : DataFrame) : Long = {
    df
      .dropDuplicates("ID_INGREDIENT")
      .count()
  }

  /**
    * Gets number of recipes in a dataframe
    * @param df input dataframe (with column ID_RECIPE)
    * @return number of ingredients
    */
  def getNRecipes(df : DataFrame) : Long = {
    df
      .dropDuplicates("ID_RECIPE")
      .count()
  }

  /**
    * Filters ingredient graph to
    * @param df
    * @param threshold
    * @return
    */
  def filterGraph(df : DataFrame, threshold : Int) : DataFrame = {
    val dfAux = df
      .groupBy("ID_INGREDIENT")
      .count()
      .orderBy(desc("count"))
      .select(col("ID_INGREDIENT").as("FID_INGREDIENT"))

    val dataRDD = spark.sparkContext.parallelize(dfAux.take(threshold))
    val dfFiltered = spark.createDataFrame(dataRDD, dfAux.schema)

    val dfRes = df.join(
      dfFiltered,
      df.col("ID_INGREDIENT") === dfFiltered.col("FID_INGREDIENT"),
      "inner"
    )
      .select(df.columns.map(col(_)) :_*)
    dfRes
  }

  /**
    * Filter top N most frequent ingredients
    * @param df input dataframe (with columns ID_RECIPE, ID_INGREDIENT)
    * @return dataframe filtered with no duplicates
    */
  def buildUndirectedGraph(df : DataFrame) : DataFrame = {
    val dfAux = buildGraphWithRecipe(df.select(df.col("ID_RECIPE"), df.col(("ID_INGREDIENT"))))
    val graph = dropDuplicates(dfAux.drop("ID_RECIPE"))
    graph
  }

  /**
    * Builds PMI graph, with PMI(a, b) = log[p(a, b) / p(a) * p(b)]
    * @param df input dataframe
    * @return output dataframe, with columns for p(a), 1/p(a) and PMI(a, b)
    */
  def buildPMIGraph(df : DataFrame) : DataFrame = {

    val dfGraph = buildGraphWithRecipe(df)

    val nRecipes = df.dropDuplicates("ID_RECIPE").count()

    val dfSingleProb = df.groupBy("ID_INGREDIENT").count().withColumn("P(INGREDIENT)", col("count") / nRecipes).drop("count")//Renamed("count", "P(INGREDIENT)")

    val dfJointProb = computeJointProbability(dfGraph, nRecipes)

    val dfUnion = dfJointProb.join(
      dfSingleProb,
      dfJointProb.col("ORIGIN") === dfSingleProb.col("ID_INGREDIENT"),
      "left"
    )
      .select(dfJointProb.columns.map(col(_))
        :+ dfSingleProb.col("P(INGREDIENT)").as("P(ORIGIN)")
        :_*)
      .join(
        dfSingleProb,
        col("DESTINATION") === dfSingleProb.col("ID_INGREDIENT"),
        "left"
      )
      .select(
        col("ORIGIN"),
        col("DESTINATION"),
        col("P(ORIGIN,DESTINATION)"),
        col("P(ORIGIN)"),
        dfSingleProb.col("P(INGREDIENT)").as("P(DESTINATION)")
        )
        .withColumn("1/P(ORIGIN,DESTINATION)", lit(1).cast(DoubleType) / col("P(ORIGIN,DESTINATION)").cast(DoubleType))
      .withColumn("PMI", log2(col("P(ORIGIN,DESTINATION)") / (col("P(ORIGIN)") * col("P(DESTINATION)"))))
      .drop("P(ORIGIN)")
      .drop("P(DESTINATION)")

    dfUnion
  }

  /**
    * Builds ingredients graph with recipe ID
    * @param df input dataframe
    * @return graph in a dataframe
    */
  def buildGraphWithRecipe(df : DataFrame) : DataFrame = {
    val dfRenamed = df.select(df.columns.map(colName => col(colName).as(s"2${colName}")) :_*)

    val dfResult = df.join(
      dfRenamed,
      df.col("ID_RECIPE") === dfRenamed.col("2ID_RECIPE") &&
        df.col("ID_INGREDIENT") =!= dfRenamed.col("2ID_INGREDIENT"),
      "inner"
    )
      .select(col("ID_RECIPE"), col("ID_INGREDIENT").as("ORIGIN"), col("2ID_INGREDIENT").as("DESTINATION"))
    dfResult
  }

  /**
    * Drop duplicates in the list of ingredients
    * @param df input dataframe
    * @return list of ingredients with no duplicates
    */
  def dropDuplicates(df : DataFrame) : DataFrame = {
    val df1 = df
      .dropDuplicates("ORIGIN", "DESTINATION")
      .orderBy("ORIGIN")
    val df2 = df1
      .select(
        col("ORIGIN"),
        col("DESTINATION"),
        when(col("ORIGIN") > col("DESTINATION"), lit(1).cast(IntegerType)).otherwise(lit(0).cast(IntegerType)).as("FLAG")
      )
      .filter(col("FLAG") === lit(0))
      .drop("FLAG")
    df2
  }

  /**
    * Computes joint probability for each pair of ingredient taking place in the
    * same recipe
    * @param df input dataframe
    * @param nRecipes number of recipes
    * @return output dataframe
    */
  def computeJointProbability(df : DataFrame, nRecipes : Long) : DataFrame = {
    val df1 = df
      .orderBy("ID_RECIPE", "ORIGIN")
    val df2 = df1
      .select(
        col("ORIGIN"),
        col("DESTINATION"),
        when(col("ORIGIN") > col("DESTINATION"), lit(1).cast(IntegerType)).otherwise(lit(0).cast(IntegerType)).as("FLAG")
      )
      .filter(col("FLAG") === lit(0))
      .drop("FLAG")
      .groupBy("ORIGIN", "DESTINATION").count().withColumn("P(ORIGIN,DESTINATION)", col("count") / nRecipes).drop("count")//withColumnRenamed("count", "P(ORIGIN,DESTINATION)")
    df2
  }

}
