package com.netcloud.bigdata.sparksql.project.pocfeature.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat}

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("test").getOrCreate()
    val dataFrame=spark.read.option("header",true).csv("data/sparksql/poc/yinshu/airline1000.csv")
    dataFrame.show()
//    val res=dataFrame.withColumn("target",col("test"))
//    val df=res.toDF()
  }

}
