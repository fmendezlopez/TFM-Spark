package es.uam.eps.tfm.fmendezlopez.utils

import org.apache.log4j.Logger

/**
  * Created by Francisco on 09/05/2017.
  */
trait Logging {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
}
