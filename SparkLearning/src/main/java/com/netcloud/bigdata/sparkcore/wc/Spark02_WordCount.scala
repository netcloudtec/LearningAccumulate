package com.netcloud.bigdata.sparkcore.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ysj
 * @date 2021/7/1 17:31
 * @version 1.0
 * 使用 reduceByKey进行单词统计 （重点熟练手写）
 */
object Spark02_WordCount {

  def main(args: Array[String]): Unit = {

    // TODO 建立和Spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1. 读取文件，获取一行一行的数据
    //    This is a wordcount test
    val lines: RDD[String] = sc.textFile("data/sparkcore/input/datas.txt")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    //    扁平化：将整体拆分成个体的操作
    //   "hello world" => hello, world, hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 将单词进行结构的转换,方便统计
    // word => (word, 1)
    val wordToOne = words.map(word => (word, 1))

    // 4. 将转换后的数据进行分组聚合
    // 相同key的value进行聚合操作
    // (word, 1) => (word, sum)
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)
    // TODO 关闭连接
    sc.stop()
  }
}
