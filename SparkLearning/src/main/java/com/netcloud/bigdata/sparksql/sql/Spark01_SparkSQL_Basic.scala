package com.netcloud.bigdata.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ysj
 * @date 2021/7/30 20:05
 * @version 1.0
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    // 创建上下文环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // 创建 SparkSession 对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
    // spark 不是包名，是上下文环境对象名
    import spark.implicits._


    // 执行逻辑操作
    // TODO 1 创建 DataFrame
    /**
     * 创建DataFrame的方式
     * 1）数据源创建：json、csv、jdbc、text
     * 2）RDD转换
     * 3）Hive Table查询返回
     */
    // 读取 json 文件创建 DataFrame
    val peopleDF: DataFrame = spark.read.json("data/sparksql/people.json")
    peopleDF.show()

    // TODO 2 DataFrame的SQL操作 :  df.sql("").show
    // DataFrame => SQL （SQL 风格语法）
    peopleDF.createOrReplaceTempView("user")
    spark.sql("select * from user").show
    spark.sql("select age, name from user").show
    spark.sql("select avg(age) from user").show

    // TODO 3 DataFrame的DSL操作 ： df.select("").show
    // DataFrame => DSL （DSL 风格语法）
    // 在使用DataFrame时，如果涉及到转换操作（某个字段值加1），需要引入转换规则（$符号或者单引号）
    peopleDF.select("age", "name").show
    peopleDF.select($"age" + 1).show
    peopleDF.select('age + 1).show

    // TODO 4 创建 DataSet
    /**
     * 1) 使用基本的序列创建DataSet
     * 2）使用样例类序列创建DataSet
     */
    // DataFrame其实是特定泛型的DataSet
    //1) 使用基本的序列创建DataSet
    val seq = Seq(1, 2, 3, 4)
    val SeqDS: Dataset[Int] = seq.toDS()
    SeqDS.show()
    //2）使用样例类序列创建DataSet
    val caseClassDS = Seq(User(1001, "New Balance", 22)).toDS()
    caseClassDS.show()
    // TODO 5 DataSet的SQL和 DSL语法操作和DataFrame一样
    caseClassDS.createOrReplaceTempView("caseClassUser")
    spark.sql("select * from caseClassUser").show()
    spark.sql("select age, name from caseClassUser").show()
    spark.sql("select avg(age) from caseClassUser").show()
    caseClassDS.select("age","name").show()

    // TODO 6 RDD、DataFrame、DataSet之间的相互转换
    // RDD <=> DataFrame
    //1) RDD.toDF("column1","column2")
    val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    //2) 一般生产环境下，通过样例类，将RDD转为DataFrame
    val caseClassDF = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDF()
    rdd.map(t=>User(t._1,t._2,t._3)).toDF.show()
    // DataFrame <=> RDD
    //RDD  返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集， 但是索引从 0 开始
    val rowRDD: RDD[Row] = caseClassDF.rdd
    rowRDD.foreach(rs=>println(rs.get(1)))

    // RDD <=> DataSet
    // 样例类转DS和样例类转为DF方式一样
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    // DataSet <=> RDD
    // 和DataFrame 转为RDD方式一样
    val userRDD: RDD[User] = ds1.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    // DataSet <=> DataFrame
    val df1: DataFrame = ds.toDF()

    // TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)


}
