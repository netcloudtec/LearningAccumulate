package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 返回每个分区的最大值
 */
object Spark02_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        // 【1，2】，【3，4】
        // 【2】，【4】
        // 传入一个iter 返回一个 iter
        val mpRDD = rdd.mapPartitions(
            iter => {
                //iter.max 转为迭代器
                List(iter.max).iterator
            }
        )
        mpRDD.collect().foreach(println)

        sc.stop()

    }
}
