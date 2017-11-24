package es.uam.eps.tfm.fmendezlopez.utils

import java.io._

import com.github.tototoshi.csv.{CSVFormat, CSVReader, CSVWriter, DefaultCSVFormat, QUOTE_MINIMAL, Quoting}

/**
  * Created by Francisco on 11/04/2017.
  */
object CSVManager {

  def openCSVWriter(path:String, name:String, separator:Char, appendMode:Boolean = true, charset : String = "UTF-8"): CSVWriter ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = separator
    }
    val pathFile = new File(path)
    if(!pathFile.exists()){
      pathFile.mkdirs()
    }
    val file = s"${pathFile}${File.separator}${name}"
    val fos = new FileOutputStream(file, appendMode)
    val osw = new OutputStreamWriter(fos, charset)
    val bw = new BufferedWriter(osw)
    CSVWriter.open(bw)
  }
  def closeCSVWriter(csvWriter: CSVWriter): Unit ={
    csvWriter.close()
  }

  def openCSVReader(file : File, separator : Char): CSVReader ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = separator
    }
    val fis = new FileInputStream(file)
    val isr = new InputStreamReader(fis)
    val br = new BufferedReader(isr)
    CSVReader.open(br)
  }

  def openCSVReaderLines(file : File, separator : Char, lines : Int): CSVReader ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = separator
    }
    val fis = new FileInputStream(file)
    val isr = new InputStreamReader(fis)
    val br = new BufferedReader(isr)
    var i = 0
    while(i < lines){
      br.readLine()
      i += 1
    }
    CSVReader.open(br)
  }

  def skipLines(csvReader: CSVReader, lines : Int, predicate : Option[Seq[String]] => Boolean) : Unit = {
    var i = 0
    var continue = true
    while(i < lines && continue){
      val line = csvReader.readNext()
      i += 1
      continue = predicate(line)
    }
  }

  def closeCSVReader(csvReader: CSVReader): Unit ={
    csvReader.close()
  }
}
