package com.betcloudai.bidata.flink.wc

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
 * @author ysj
 * @date 2021/8/7 21:44
 * @version 1.0
 */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 从文件中读取数据
    val inputDS: DataSet[String] = env.readTextFile("data/wc/hello.txt")
    inputDS.print()
  }

}
