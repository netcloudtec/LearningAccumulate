package com.netcloud.bigdata.sparksql.project.pocfeature.utils

import java.io._
import java.util.Properties

import org.apache.commons.cli.{BasicParser, CommandLine, Options}

import scala.collection.JavaConverters._
import scala.collection.mutable

object ConfInitUtils {

  def close(close: AutoCloseable): Unit = if (close != null) {
    try {
      close.close()
    } catch {
      case _: Exception =>
    }
  }

  /**
    * 配置属性加载为Map
    *
    * @param confPath 配置属性文件路径
    * @return 配置键值对Map
    */
  def parseConfMap(confPath: String): mutable.Map[String, String] = {
    val in = new FileInputStream(confPath)
    parseProperties(in)
  }

  /**
    * 从打包resources文件解析键值对到map
    *
    * @param resourceName 资源名
    * @return 配置键值对Map
    */
  def parseResourceConfMap(resourceName: String): mutable.Map[String, String] = {
    val in = getClass.getClassLoader.getResourceAsStream(resourceName)
    parseProperties(in)
  }

  /**
    * 解析输入的流文件到Map
    *
    * @param in InputStream 输入流
    * @return Map对象
    */
  def parseProperties(in: InputStream): mutable.Map[String, String] = {
    val confProp = new Properties()
    var reader: Reader = null
    try {
      reader = new InputStreamReader(in, "utf-8")
      confProp.load(reader)
    } catch {
      case _: Exception =>
    } finally {
      close(reader)
    }
    confProp.asScala
  }

  /**
    * 过滤出需要的配置
    *
    * @param prefix  String key前缀正则表达式
    * @param confMap scala.collection.mutable.Map[String, String] 配置Map
    * @return scala.collection.mutable.HashMap[String, String] 配置Map
    */
  def confMapFilter(prefix: String, confMap: mutable.Map[String, String]): mutable.HashMap[String, String] = {
    val conf_regex = s"$prefix(.*)".r
    val resultMap = new mutable.HashMap[String, String]()
    confMap.foreach(x => x._1 match {
      case conf_regex(key) => resultMap += ((key, x._2))
      case _ => null
    })
    resultMap
  }

  /**
    * apache命令行解析工具解析传入参数
    *
    * @param args main传入参数
    * @param options Options 所有项 opt要符合java变量名命名规范
    * @return
    */
  def cliParse(args: Array[String], options: Options): CommandLine = {
    //由于hadoop集群所用cli版本低使用BasicParser解析输入参数
    val parser: BasicParser = new BasicParser()
    val cli = parser.parse(options, args)
    if (args.length == 0 || cli.hasOption("h")) {
      options.getOptions.asScala.foreach(println)
    }
    cli
  }

  /**
    * 是否 spark-submit 提交的任务
    *
    * @return Boolean
    */
  def isSparkSubmit: Boolean = {
    val spark_submit_flag = System.getProperty("SPARK_SUBMIT")
    if (spark_submit_flag != null) spark_submit_flag.toBoolean else false
  }
}
