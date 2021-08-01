package com.netcloud.bigdata.sparksql.project.pocfeature.utils

import java.util.Properties
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PropertiesUtil {

  def loadProperties(propFile: String) = {
    val prop = new Properties
    try {
      val is = this.getClass.getClassLoader.getResourceAsStream(propFile)
      prop.load(is)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    prop
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val prop = loadProperties("config.properties")
    println(prop.getProperty("keycolumns"))
  }

}
