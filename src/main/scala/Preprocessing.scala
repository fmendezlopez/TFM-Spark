import java.io._
import java.nio.charset.Charset

import es.uam.eps.tfm.fmendezlopez.utils.SparkUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.convert._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by franm on 14/07/2017.
  */
object Preprocessing {

  val options : Map[String, String] = Map(
    "sep" -> "|",
    "encoding" -> "UTF-8",
    "header" -> "true"
  )

  def main(args: Array[String]): Unit = {
    upgradeDataset

  }
  def changeCharset = {
    //val inputPath = "C:\\Users\\franm\\Desktop\\ejecuciones\\extractor_jar\\Extractor\\output\\stage2\\fran-laptop"
    val inputPath = "C:\\Users\\franm\\Desktop\\prueba\\in"
    val inputFile = new File(inputPath)
    val charset1 = Charset.forName("UTF-16")
    val charset2 = Charset.forName("UTF-8")
    //val outputPath = "C:\\Users\\franm\\Desktop\\ejecuciones\\extractor_jar\\Extractor\\output\\stage2\\utf16"
    val outputPath = "C:\\Users\\franm\\Desktop\\prueba\\out"

    inputFile.listFiles().toSeq.foreach(file => {
      val fis = new FileInputStream(file.getAbsolutePath)
      val isr = new InputStreamReader(fis, charset1)
      val br = new BufferedReader(isr)

      val outputFile = s"${outputPath}${File.separator}${file.getName}"
      val fos = new FileOutputStream(outputFile)
      val osw = new OutputStreamWriter(fos, charset2)
      val bw = new BufferedWriter(osw)

      var line = ""
      var continue = true
      while(continue){
        line = br.readLine()
        if(line == null)
          continue = false
        else{
          bw.write(s"${line}\r\n")
        }
      }
      br.close()
      bw.close()
    })

  }

  def upgradeDataset = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Preprocessing")
      .getOrCreate()

    val inputPath = "C:\\Users\\franm\\IdeaProjects\\TFM\\Extractor\\src\\main\\resources\\input\\dataset"
    val outputPath = "C:\\Users\\franm\\IdeaProjects\\TFM-Spark\\src\\main\\resources\\input\\upgraded_dataset"

    val fav = readCSV(inputPath, "favourites", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("fav"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID"))
    val publications = readCSV(inputPath, "publications", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("recipes"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID").as("USER_ID"))
    val madeit = readCSV(inputPath, "madeit", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("madeit"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("USER_ID").as("USER_ID"))
    val reviews = readCSV(inputPath, "reviews", Some(options), None)
      .withColumn("RECIPE_TYPE", lit("review"))
      .select(col("RECIPE_TYPE"), col("RECIPE_ID"), col("AUTHOR_ID").as("USER_ID"))
    val ingredients_aux = readCSV(inputPath, "ingredients", Some(options), None)
    val ingredients = ingredients_aux.select(Seq(col("ID").as("ID_INGREDIENT")) ++ ingredients_aux.columns.filter(_ != "ID").map(col) :_*)
    val recipes_aux = readCSV(inputPath, "recipes", Some(options), None)
    val recipes = recipes_aux.select(Seq(col("ID").as("RECIPE_ID")) ++ recipes_aux.columns.filter(_ != "ID").map(col) :_*)

    val user_recipes = fav.union(publications).union(madeit).union(reviews)

    writeCSV(user_recipes, outputPath, "user-recipe", Some(options))
    writeCSV(ingredients, outputPath, "ingredients", Some(options ))
    writeCSV(recipes, outputPath, "recipes", Some(options ))
  }
}
