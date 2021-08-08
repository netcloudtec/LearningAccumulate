package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform3 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - (Key - Value类型)

        val rdd = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("b", 3),
            ("b", 4), ("b", 5), ("a", 6)
        ),2)

        // 获取相同key的数据的平均值 => (a, 3),(b, 4)
        //第一个参数：相同key的初始值（0，0） 第一个0表示所计算的初始值；第二个0表示key出现的次数
        //第二个参数：（t,v）表示初始值的tuple(0,0) v表示的是要计算key的value
        //t._1 + v :数据相加；t._2 + 1： 次数相加
        //第三个参数： (t1, t2) ：表示的两个分区，每个分区的数据都是一个tuple，tuple的第一个元素是数据值
        // tuple的第二个元素是 次数值
        // 返回值的 String是key ；(Int, Int) 表示的数据值和次数值
        val newRDD : RDD[(String, (Int, Int))] = rdd.aggregateByKey( (0,0) )(
            ( t, v ) => {
                (t._1 + v, t._2 + 1)
            },
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )
        // 求解key的平均值
        val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
            case (num, cnt) => {
                num / cnt
            }
        }
        resultRDD.collect().foreach(println)





        sc.stop()

    }
}
