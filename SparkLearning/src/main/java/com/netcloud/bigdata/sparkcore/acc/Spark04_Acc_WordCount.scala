package com.netcloud.bigdata.sparkcore.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_WordCount {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
        val sc = new SparkContext(sparConf)

        val rdd = sc.makeRDD(List("hello", "spark", "hello"))

        // 累加器 : WordCount
        // 创建累加器对象
        val wcAcc = new MyAccumulator()
        // 向Spark进行注册，第二个参数为自定义累加器名称
        sc.register(wcAcc, "wordCountAcc")

        rdd.foreach(
            word => {
                // 数据的累加（使用累加器）
                wcAcc.add(word)
            }
        )
        // 获取累加器累加的结果
        println(wcAcc.value)
        sc.stop()

    }
    /*
      自定义数据累加器：WordCount
      1. 继承AccumulatorV2, 定义泛型
         IN : 累加器输入的数据类型 String
         OUT : 累加器返回的数据类型 mutable.Map[String, Long]
      2. 重写方法（6）
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

        private var wcMap = mutable.Map[String, Long]()

        // 累加器是否为初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }
        // 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator()
        }

        // 重置累加器
        override def reset(): Unit = {
            wcMap.clear()
        }

        // 获取累加器的输入，向累加器中增加数据  (In)
        override def add(word: String): Unit = {
            // 查询 map 中是否存在相同的单词
            // 如果有相同的单词，那么单词的数量加 1
            // 如果没有相同的单词，那么在 map 中增加这个单词
            val newCnt = wcMap.getOrElse(word, 0L) + 1
            wcMap.update(word, newCnt)
        }

        // Driver合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

            val map1 = this.wcMap
            val map2 = other.value
            map2.foreach{
                case ( word, count ) => {
                    val newCount = map1.getOrElse(word, 0L) + count
                    map1.update(word, newCount)
                }
            }
        }
        // 累加器结果
        override def value: mutable.Map[String, Long] = {
            wcMap
        }
    }
}
