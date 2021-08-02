package com.netcloud.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 创建一个至少具有两个工作线程（working thread）并且批次间隔为 3 秒的本地 StreamingContext。
 * master至少需要2个CPU核，以避免出现任务饿死的情况。一个用来接收数据，一个用来计算。
 * localhost[*]:到底多少个个CPU核数和自己的机器配置有关
 */
object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // TODO 逻辑处理
        // 获取端口数据
        // 这里的 lines DStream 表示从数据server接收到的数据流。在这个离散流（DStream）中的每一条记录都是一行文本（text）
        // 接下来，我们就需要把这些文本行按空格分割成单词
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        // 将每一行分割成多个单词
        val words = lines.flatMap(_.split(" "))
        // 对每一批次中的单词进行计数
        val wordToOne = words.map((_,1))
        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)
        // 将该DStream产生的RDD的头十个元素打印到控制台上
        // 注意：必需要触发 action（很多初学者会忘记触发 action 操作，导致报错：No output operations registered, so nothing to execute）
        wordToCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
        //ssc.stop()
        // 1. 启动流式计算采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
