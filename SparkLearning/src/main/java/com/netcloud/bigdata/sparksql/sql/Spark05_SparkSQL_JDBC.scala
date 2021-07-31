package com.netcloud.bigdata.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://123.57.75.98:3306/datax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Sunmnet@123")
      .option("dbtable", "student")
      .load()
//    df.show

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://123.57.75.98:3306/datax")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "Sunmnet@123")
      .option("dbtable", "student2")
      .mode(SaveMode.Append)
      .save()

    // TODO 关闭环境
    spark.close()
  }
}
