package com.netcloud.bigdata.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author ysj
 * @date 2021/7/31 15:53
 * @version 1.0
 */
/**
 * 通用的数据加载和保存方式
 */
object Spark04_SparkSQL_readAndWrite {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("readAndwrite")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val parquetDF = spark.read.load("data/sparksql/users.parquet")
    val jsonDF = spark.read.format("json").load("data/sparksql/people.json")
    val jsonBakDF = spark.read.json("data/sparksql/people.json")

    // 默认保存parquet格式
    parquetDF.write.save("data/sparksql/output/users.parquet")
    // df.write.format().mode().save()
    jsonDF.write.format("json").mode(SaveMode.Overwrite).save("data/sparksql/output/people.json")
    // df.write.mode().json()
    jsonBakDF.write.mode(SaveMode.Overwrite).json("data/sparksql/output/peoplebak.json")
  }

}
