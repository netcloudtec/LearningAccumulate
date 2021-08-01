package com.netcloud.bigdata.sparksql.project.pocfeature.utils

import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, TimestampType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row, Column, SaveMode}
import java.sql.Timestamp
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import scala.collection.mutable.ArrayBuffer

object AggByDay {
  def makeColumnByWindow(columns: Seq[Column],
                          aggOpts: Seq[String],
                          group: Array[(WindowSpec, String)]): TraversableOnce[Column] = {
    val cols = ArrayBuffer.empty[Column]

    //对每个开窗函数
    for (indices <- group) {
      //对每列名称
      for (elem <- columns) {
        val elem_name = elem.toString().trim().split("`")
        val name1 = if(elem_name.size.>(1)) elem_name.drop(1).mkString else elem_name.head
        val name2 = name1.split("_").dropRight(1).mkString("_")

        //每个操作函数
        aggOpts.foreach { funName =>
          cols += callUDF(funName, elem).over(indices._1).as(s"${name2}_${indices._2}")
        }
      }
    }
    cols
  }


  /*
   * 统计字符串数组列中和最后一条相同的总共有多少条
   *
   * @param array 列表达式数组
   * @return 返回转化后的数组
   */
  def sameValueCount(array: TraversableOnce[Column]): TraversableOnce[Column] = {
    val sameValueTimesUDF = udf {
      (seq: Seq[Any]) =>
        seq.count(_.equals(seq.last))
    }
    array.map{
      columnExpr =>
        val elem_name = columnExpr.toString().trim().split("`")
        val name = if (elem_name.size.>(1)) elem_name.drop(1).mkString else elem_name.head
        sameValueTimesUDF(columnExpr).as(name + "_same_num")
    }
  }

  def aggByTwoKey(inputRawDf: Dataset[Row]) = {
    val timeInterval = Array(
      60 * 60 * 24 * 7,
      60 * 60 * 24 * 15
    )

    val column_user_acptor_amt = col("Consume_AMT").cast(DoubleType).as("user_acptor_amt")
    val inputRawDfGroup = inputRawDf.groupBy("SD_PAN", "SD_RETL_ID", "LOCAL_DATE")
                                    .agg(sum(column_user_acptor_amt).as("user_acptor_amt_sum_24Hours"),
                                         count(column_user_acptor_amt).as("user_acptor_amt_count_24Hours"),
                                         avg(column_user_acptor_amt).as("user_acptor_amt_avg_24Hours")
                                         )
                                    .withColumn("LOCAL_UNIX_TIMESTAMP", unix_timestamp(col("LOCAL_DATE")))

    val windowPartitionByCardNumAndAcptorOrderByTimestamp = Window.partitionBy(col("SD_PAN"), col("SD_RETL_ID")).orderBy(col("LOCAL_UNIX_TIMESTAMP"))

    val timeGroupByAcptorAndCardNum = timeInterval.map {
      time => {
        val winSpec = windowPartitionByCardNumAndAcptorOrderByTimestamp.rangeBetween(1 - time, 0)
        val timeValue = time./(3600)
        val timeStr = timeValue match {
          case caseValue if (caseValue.>(24)) => timeValue./(24).toString().+("Day")
          case 0 => time./(60).toString().+("Min")
          case _ => timeValue.toString().+("Hour")
        }
        (winSpec, timeStr)
      }
    }

    val columnOneStep = makeColumnByWindow(Seq(col("user_acptor_amt_sum_24Hours"),
        col("user_acptor_amt_count_24Hours")), Seq("sum"), timeGroupByAcptorAndCardNum)

    inputRawDfGroup.select(Seq("SD_PAN", "SD_RETL_ID", "LOCAL_DATE").map(col).++(columnOneStep):_*)
  }

  def addDeriveVarForRawDf(inputRawDf: Dataset[Row]) = {
    val mTimesAmtCols1 = Seq(
        //外币消费金额
        when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"), col("Consume_AMT")).as("No_Rmb_AMT"),
        //人民币消费金额
        when(col("SD_ORIG_CRNCY_CDE").equalTo("156"), col("Consume_AMT")).as("Rmb_AMT")
    )
    val mTimesAmtCols2 = Seq(
        //国内商户消费金额
        when(col("SD_ORIG_WIR_CNTRY_CDE").equalTo("156"), col("Consume_AMT")).as("china_amt"),
        //国外商户授权金额
        when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"), col("Consume_AMT")).as("abroad_amt"),
        //近N小时高风险国家A授权金额指标
        when(col("SD_ORIG_WIR_CNTRY_CDE").isin("840", "372", "250"), col("Consume_AMT")).as("high_risk_country_a_amt"),
        //近N小时高风险国家C授权金额指标
        when(col("SD_ORIG_WIR_CNTRY_CDE").isin("076", "484", "356", "428", "124", "528"), col("Consume_AMT")).as("high_risk_country_c_amt"),
        //国外高风险MCC_CODE A 授权金额
        when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156") && col("SD_RETL_SIC_CDE").isin("5735", "4121"), col("Consume_AMT")).as("high_risk_mcc_code_a_amt"),
        //国外高风险MCC_CODE C 授权金额
        when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156") && col("SD_RETL_SIC_CDE").isin("4816", "5047", "5968", "0780", "5967", "5816"), col("Consume_AMT")).as("high_risk_mcc_code_c_amt")
    )
    val mTimesAmtCol = mTimesAmtCols1.++(mTimesAmtCols2)

    inputRawDf.select(Seq(col("*")).++(mTimesAmtCol):_*)
  }

  def aggByOneKey(inputRawDf: Dataset[Row]) = {
    val timeInterval = Array(
      60 * 60 * 24 * 7,
      60 * 60 * 24 * 15,
      60 * 60 * 24 * 30
    )

    val addColumn = ArrayBuffer.empty[Column]
    val calcVarColumnA = Seq("Cash_AMT", "Consume_AMT", "Reyment_AMT").map(str => {
      val colStr = col(str)

      val str_sum = sum(colStr).as(s"${str}_sum_24Hour")
      val str_count = count(colStr).as(s"${str}_count_24Hour")
      val str_max = max(colStr).as(s"${str}_max_24Hour")

      addColumn.+=(str_sum)
      addColumn.+=(str_count)
      addColumn.+=(str_max)

      Seq(str_sum, str_count, str_max)
    }).flatMap(f => f)

    val calcVarColumnB = Seq("No_Rmb_AMT", "Rmb_AMT", "china_amt", "abroad_amt", "high_risk_country_a_amt", "high_risk_country_c_amt", "high_risk_mcc_code_a_amt", "high_risk_mcc_code_c_amt").map(str => {
      val colStr = col(str).cast(DoubleType)

      val str_sum = sum(colStr).as(s"${str}_sum_24Hour")
      val str_count = count(colStr).as(s"${str}_count_24Hour")
      val str_max = max(colStr).as(s"${str}_max_24Hour")
      val str_avg = avg(colStr).as(s"${str}_avg_24Hour")

      addColumn.+=(str_sum)
      addColumn.+=(str_count)
      addColumn.+=(str_max)
      addColumn.+=(str_avg)

      Seq(str_sum, str_count, str_max, str_avg)
    }).flatMap(f => f)

    //val udfGroupCount = udf(lines: Seq[Any] => {
    //  val countSeq = lines.map(f => (f, 1)).groupBy(_._1).mapValues(_.size)
    //  val last = lines.last
    //  (countSeq, last)
    //})

    val calcVarColumnC = Seq("Consume_AMT", "SD_CITYNO", "SD_RETL_ID").map(str => {
      val colStr = col(str)
      val str_collect_list = collect_list(colStr).as(s"${str}_collect_list_24Hour")

      addColumn.+=(str_collect_list)
      str_collect_list
    })

    val calcVarColumnD = sum(col("Consume_AMT").*(col("Consume_AMT"))).as(s"Consume_AMT_squart_sum_24Hour")
    addColumn.+=(calcVarColumnD)

    val df = addDeriveVarForRawDf(inputRawDf).groupBy("SD_PAN", "LOCAL_DATE")
                                            .agg(addColumn(0), addColumn(1), addColumn(2), addColumn(3),
                                                 addColumn(4), addColumn(5), addColumn(6), addColumn(7),
                                                 addColumn(8), addColumn(9), addColumn(10), addColumn(11),
                                                 addColumn(12), addColumn(13), addColumn(14), addColumn(15),
                                                 addColumn(16), addColumn(17), addColumn(18), addColumn(19),
                                                 addColumn(20), addColumn(21), addColumn(22), addColumn(23),
                                                 addColumn(24), addColumn(25), addColumn(26), addColumn(27),
                                                 addColumn(28), addColumn(29), addColumn(30), addColumn(31),
                                                 addColumn(32), addColumn(33), addColumn(34), addColumn(35),
                                                 addColumn(36), addColumn(37), addColumn(38), addColumn(39),
                                                 addColumn(40), addColumn(41), addColumn(42), addColumn(43),
                                                 addColumn(44)
                                                )
                                             .withColumn("LOCAL_UNIX_TIMESTAMP", unix_timestamp(col("LOCAL_DATE")))
    //df.columns.foreach(println)
    //df.show(false)
    val windowPartitionByCardNumOrderByTimestamp = Window.partitionBy(col("SD_PAN")).orderBy(col("LOCAL_UNIX_TIMESTAMP"))
    //构建多个以用户分组时间戳排序限定时间范围的窗口，提供开窗函数使用
    val timeGroupByCardNum = timeInterval.map {
      time => {
        val winSpec = windowPartitionByCardNumOrderByTimestamp.rangeBetween(1 - time, 0)
        val timeValue = time./(3600)
        val timeStr = timeValue match {
          case caseValue if (caseValue.>(24)) => timeValue./(24).toString().+("Days")
          case 0 => time./(60).toString().+("Min")
          case _ => timeValue.toString().+("Hour")
        }
        (winSpec, timeStr)
      }
    }
    val addCalcColumn = ArrayBuffer.empty[Column]

    val firstSeq = Seq("Cash_AMT_sum_24Hour", "Cash_AMT_count_24Hour", "Consume_AMT_sum_24Hour",
                       "Consume_AMT_count_24Hour", "Reyment_AMT_sum_24Hour", "Reyment_AMT_count_24Hour",
                       "No_Rmb_AMT_sum_24Hour", "No_Rmb_AMT_count_24Hour", "Rmb_AMT_sum_24Hour",
                       "Rmb_AMT_count_24Hour", "china_amt_sum_24Hour", "china_amt_count_24Hour",
                       "abroad_amt_sum_24Hour", "abroad_amt_count_24Hour", "high_rish_country_a_amt_sum_24Hour",
                       "high_risk_country_a_amt_count_24Hour", "high_risk_country_c_amt_sum_24Hour",
                       "high_risk_country_c_amt_count_24Hour", "high_risk_mcc_code_a_amt_sum_24Hour",
                       "high_risk_mcc_code_a_amt_count_24Hour", "high_risk_mcc_code_c_amt_sum_24Hour",
                       "high_risk_mcc_code_c_amt_count_24Hour", "Consume_AMT_squart_sum_24Hour").map(col)

    addCalcColumn.++=(makeColumnByWindow(firstSeq, Seq("sum"), timeGroupByCardNum))

    val secondColumn = Seq("Cash_AMT_max_24Hour", "Consume_AMT_max_24Hour", "Reyment_AMT_max_24Hour",
                           "No_Rmb_AMT_max_24Hour", "Rmb_AMT_max_24Hour", "china_amt_max_24Hour",
                           "abroad_amt_max_24Hour", "high_risk_country_a_amt_max_24Hour",
                           "high_risk_country_c_amt_max_24Hour", "high_risk_mcc_code_a_amt_max_24Hour",
                           "high_risk_mcc_code_c_amt_max_24Hour").map(col)

    addCalcColumn.++=(makeColumnByWindow(secondColumn, Seq("max"), timeGroupByCardNum))

    df.select(Seq(col("*")).++(addCalcColumn):_*).columns.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
                              .set("Spark.debug.maxToStringFields", "100").setMaster("local[4]")

    val sparksession = SparkSession.builder().config(conf).getOrCreate()
    val inputFile = "data/ValidationResultData_2018_07-04_2018-07-18_bigcard_15"
    val inputRawDf = sparksession.read.option("mergeSchema", true).parquet("inputFile")
                                 .withColumn("MD_TRANS_AMT3", col("MD_TRANS_AMT3").cast(DoubleType))

    aggByOneKey(inputRawDf)
  }
}
