package com.netcloud.bigdata.sparksql.project.pocfeature.utils

import scala.io.Source
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, TimestampType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row, Column}
import java.sql.Timestamp
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar

object UtilitySource {
  /*
   * 建立mcc code 对应索引映射
   */
  def mccCodeIndexClass(mccFile: String) = {
    Source.fromFile(mccFile).getLines().filter(!_.startsWith("#"))
          .map(_.split("=").last.trim().split(",").map(_.trim()))
          .zipWithIndex.flatMap{case (k, v) => k.map(f => (f, v))}
          .toMap
  }

  /*
   * df 添加mcc大类索引列
   */
  def dfAddMccIndexColumn(df: Dataset[Row], mcc_map: Map[String, Int], colname: String) = {
    val maxValue = mcc_map.map(_._2).max.+(1)
    val mcc_big_cate_udf = udf {
      (mcc_class: String) => mcc_map.getOrElse(mcc_class, maxValue)
    }
    (df.withColumn("MCC_CLASS_INDEX", mcc_big_cate_udf(col(colname))), maxValue)
  }

  /*
   * 时间向前索引一个月计算相关变量
   */
  def dateIndexOneMonth(endDate: String) = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse(endDate)
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, -1)
    calendar.add(Calendar.DATE, 1)
    val middleTime = calendar.getTime
    val middleDate = dateFormat.format(middleTime)
    val gapDay = (date.getTime.-(middleTime.getTime))./(60 * 60 * 24 * 1000).+(1)
    calendar.add(Calendar.MONTH, -1)
    val outputBeginDate = dateFormat.format(calendar.getTime)
    (outputBeginDate, gapDay, middleDate)
  }
  /*
   * 根据输入天数计算相应天数特征
   */
  def dateIndexWithInputDays(endDate: String, calcDay: Int) = {
    val calendar = Calendar.getInstance
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse(endDate)
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -(calcDay.-(1)))
    val obtainDataTime = calendar.getTime
    val obtainDataDate = dateFormat.format(obtainDataTime)
    calendar.add(Calendar.MONTH, -1)
    val outputBeginDate = dateFormat.format(calendar.getTime)
    (outputBeginDate, obtainDataDate)
  }

  def main(args: Array[String]) = {
    val mcc_file = "./doc/mcc_desc.txt"
    val result = mccCodeIndexClass(mcc_file)
    result.foreach(println)
  }
}
