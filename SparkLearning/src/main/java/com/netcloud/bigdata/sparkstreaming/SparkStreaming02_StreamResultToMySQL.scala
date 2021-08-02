package com.netcloud.bigdata.sparkstreaming

import java.sql.DriverManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author ysj
 * @date 2021/8/2 00:56
 * @version 1.0
 */
object SparkStreaming02_StreamResultToMySQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]") // 启动至少两个线程，一个接收数据、一个处理数据
      .appName("streamingToMySQL").getOrCreate()

    val sc = spark.sparkContext
    // 每 5s 生成一个 batch
    val ssc = new StreamingContext(sc, Seconds(3))
    //接收 TCP sockets 数据
    val lines = ssc.socketTextStream("localhost", 9999)
    // 读取的行数据按照空格切分为单词数组，每个单词记为 1 然后再进行聚合
    val words = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    // 聚合后的结果遍历 对当前RDD中每个分区的数据进行处理
    words.print()
    words.foreachRDD(rdd => rdd.foreachPartition(line => {
      // 数据库连接 每处理一个分区时候进行连接 而不是每条数据进行连接 减少数据库连接次数提高执行效率
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager
        .getConnection("jdbc:mysql://123.57.75.98:3306/sparkstreaming_result", "root", "Sunmnet@123")
      try {
        // 遍历分区中的数据
        for (row <- line) {
          val sql = "insert into webcount(titlename,total_count)values('" + row._1 + "'," + row._2 + ")"
          conn.prepareStatement(sql).executeUpdate()
        }
      } finally {
        conn.close()
      }
    }))
    ssc.start() // 启动流式计算 否则程序无法执行
    ssc.awaitTermination() // 等待直到计算终止、程序挂起一致执行
  }
}
