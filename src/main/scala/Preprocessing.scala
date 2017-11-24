import java.io._
import java.nio.charset.Charset

import scala.collection.convert._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by franm on 14/07/2017.
  */
object Preprocessing {

  def main(args: Array[String]): Unit = {
    changeCharset

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
}
