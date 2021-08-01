package com.netcloud.bigdata.sparksql.project.pocfeature.transform

import java.util.Properties

import com.netcloud.bigdata.sparksql.project.pocfeature.utils.PropertiesUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 表字段初步处理选出要用的字段存为parquet格式方便使用
 * 根据实际情况cast字段的类型
 */
object DataReadToParquet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val propFile = args(0)

    val prop = PropertiesUtil.loadProperties(propFile)
    val isLocal = prop.getProperty("isLocal")
    val funcName = prop.getProperty("funcName")
    println("propFile: " + propFile + "\n" + "isLocal: " + isLocal + "\n" + "funcName: " + funcName)

    val sparkConf = SparkSession.builder().appName(s"spark_data_read_${funcName}")

    isLocal match {
      case "true" => sparkConf.master("local")
      case _ => sparkConf
    }
    val spark = sparkConf.getOrCreate()

    /**
     * 模式匹配:根据指定的方法名，处理不同的数据集
     */
    funcName match {
      case "train" => run(prop, spark)
      case "test" => run(prop, spark)
      case "black" => processBlackFile(prop, spark)
    }
  }

  /**
   * 加载数据为一个DataFrame
   * @param prop
   * @param spark
   * @return
   */
  def readFile(prop: Properties, spark: SparkSession): DataFrame = {
    val sep = if (prop.containsKey("sep")) prop.getProperty("sep") else "," // 指定分隔符
    val encoding = if (prop.containsKey("encoding")) prop.getProperty("encoding") else "utf-8" // 指定编码
    val input = prop.getProperty("source_input") // 指定输入数据源的路径
    val detailColumns = prop.getProperty("detail_columns") // 指定列名

    //根据文件编码格式读入数据并分割数据转化为Row
    /**
     * 1）通过newAPIHadoopFile API读取数据文件为 tuple（key,value）类型 key是行偏移量 value是每行的数据
     * 2）new String(pair._2.getBytes, 0, pair._2.getLength, encoding) 将每行数据进行编码
     * 3）将数据按照指定分隔符切分，然后转为seq集合
     * 4）Row.fromSeq（）将seq集合转为RDD[Row]
     */
    val rdd: RDD[Row]=spark.sparkContext.newAPIHadoopFile[LongWritable, Text, TextInputFormat](input).map(
        pair => Row.fromSeq(new String(pair._2.getBytes, 0, pair._2.getLength, encoding).split(sep, -1).map(_.trim).toSeq)
      )
    //RDD[Row] to DataFrame
    val fields = detailColumns.split(", ").map(str => StructField(str, StringType))
    //新增时间字段，日期LOCAL_DATE，时间LOCAL_TIME
    spark.createDataFrame(rdd, StructType(fields))
  }

  /**
   *
   * @param prop
   * @param spark
   */
  def run(prop: Properties, spark: SparkSession): Unit = {
    val saveMode = if (prop.containsKey("saveMode")) prop.getProperty("saveMode") else "default"
    val output = if (prop.containsKey("source_output")) prop.getProperty("source_output") else prop.getProperty("source_input") + "parquet"

    /**
     * SparkSQL
     * withColumn()：表示新加一列
     * concat(col(),col()) :表示将两列连接
     * rlike(regx): 使用正则表达式进行数据匹配  ^\d+$ 非负数
     * filter() :对数据行进行过滤 返回满足条件（true）的数据
     * drop(col()): 对数据列进行删除
     *
     * concat_ws() :按照指定的字符连接
     * date_format():日期格式化
     *
     */
    val dataFrame = readFile(prop, spark).withColumn("isErrorColumn", concat(col("SD_PAN"), col("SD_ORIG_WIR_CNTRY_CDE")).rlike("""^\d+$"""))
      .filter(col("isErrorColumn"))
      .drop("isErrorColumn")
      .withColumn("DD_DATE_COPY", concat(concat_ws("-", substring(col("DD_DATE"), 0, 4), substring(col("DD_DATE"), 5, 2), substring(col("DD_DATE"), 7, 2)), substring(col("DD_DATE"), 9, 13)))
      .withColumn("LOCAL_DATE", date_format(col("DD_DATE_COPY"), "yyyy-MM-dd"))
      .withColumn("LOCAL_TIME", date_format(col("DD_DATE_COPY"), "HH:mm:ss"))
    //先保存进行过错位处理和增加时间字段之后的数据
    /**
     * 将DataFrame按照指定的分区写到磁盘
     */
    dataFrame
      .write
      .partitionBy("LOCAL_DATE")
      .mode(saveMode)
      .parquet(output)
    spark.stop()
  }
  def processBlackFile(prop: Properties, spark: SparkSession) {
    val saveMode = if (prop.containsKey("saveMode")) prop.getProperty("saveMode") else "default"
    val output = if (prop.containsKey("source_output")) prop.getProperty("source_output") else prop.getProperty("source_input") + "parquet"
    val keycolumns = prop.getProperty("keycolumns")
    readFile(prop, spark).dropDuplicates(Seq(keycolumns.split(", "): _*)).write.mode(saveMode).parquet(output)
  }
}
