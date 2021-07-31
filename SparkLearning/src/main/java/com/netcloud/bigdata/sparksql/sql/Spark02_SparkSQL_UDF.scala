package com.netcloud.bigdata.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // TODO 1 :创建DataFrame
    val df = spark.read.json("data/sparksql/people.json")
    // TODO 2 :创建临时表
    df.createOrReplaceTempView("people")
    //TODO 3 :注册UDF
    // 注册用户自定义函数： name 前面加一个前缀 Name:
    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })
    //TODO 4：应用UDF
    spark.sql("select age, prefixName(name) from people").show

    // TODO 关闭环境
    spark.close()
  }
}
