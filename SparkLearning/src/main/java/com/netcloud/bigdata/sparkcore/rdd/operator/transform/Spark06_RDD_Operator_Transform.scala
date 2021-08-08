package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - groupBy
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

       val oridalPartitionRDD= rdd.mapPartitionsWithIndex(
            (index,iter)=>{
                iter.map(
                    num=>{
                        (index,num)
                    }
                )
            }
        )
        oridalPartitionRDD.collect().foreach(println)

        /**
         * 输出结果：
         * (0,1)
         * (0,2)
         * (1,3)
         * (1,4)
         * 即：1，2 元素在0号分区 3，4元素在1号分区
         */


        // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
        // 相同的key值的数据会放置在一个组中
        def groupFunction(num:Int) = {
            num % 2
        }

        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
        val par=groupRDD.mapPartitionsWithIndex(
            (index,iter)=>{
                iter.map(
                    num=>{
                        (index,num)
                    }
                )
            }
        )
        par.collect().foreach(println)
        /**
         * 输出结果：
         * (0,(0,CompactBuffer(2, 4)))
         * (1,(1,CompactBuffer(1, 3)))
         * 即：2，4 元素在0号分区 1，3元素在1号分区
         */
        sc.stop()

    }
}
