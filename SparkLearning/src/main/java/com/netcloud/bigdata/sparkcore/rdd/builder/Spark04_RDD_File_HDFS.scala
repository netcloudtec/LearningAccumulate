package com.netcloud.bigdata.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ysj
 * @date 2021/7/21 19:14
 * @version 1.0
 */
object Spark04_RDD_File_HDFS {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD

    val rdd = sc.textFile("hdfs://10.238.251.4:8020/user/bigtable.lzo")

    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()
  }
}
