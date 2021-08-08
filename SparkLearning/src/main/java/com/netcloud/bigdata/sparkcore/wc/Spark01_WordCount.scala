package com.netcloud.bigdata.sparkcore.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ysj
 * @date 2021/7/1 17:06
 * @version 1.0
 * 使用 groupBy 进行单词统计 作为了解
 */
object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    // TODO 建立和Spark框架的连接
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1. 读取文件，获取一行一行的数据
    //    hello world
    val lines: RDD[String] = sc.textFile("data/sparkcore/input/datas.txt")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    //    即扁平化：将整体拆分成个体的操作
    //   "hello world" => hello, world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3. 将数据根据单词进行分组，便于统计
    //    (hello, hello, hello), (world, world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 4. 对分组后的数据进行转换
    //    (hello, hello, hello), (world, world)
    //    (hello, 3), (world, 2)
    //    case 模式匹配
    val wordToCount = wordGroup.map {
      case ( word, list ) => {
        (word, list.size)
      }
    }
    // 5. 将转换的结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    // TODO 关闭连接
    sc.stop()
  }


}
