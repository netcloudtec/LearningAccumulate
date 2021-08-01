package com.netcloud.bigdata.sparksql.project.pocfeature.transform

import java.util.Properties

import com.netcloud.bigdata.sparksql.project.pocfeature.utils.{PropertiesUtil, UtilitySource}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object ContinuousVarCalcNew {

  //要计算的时间维度数组
  val timeInterval = Array(
    1,
    12,
    24,
    24 * 7,
    24 * 15,
    24 * 30
  )

  //构建以用户分组时间戳排序的窗口提供开窗函数使用
  val windowPartitionByCardNumOrderByTimestamp = Window.partitionBy(col("SD_PAN")).orderBy(col("LOCAL_UNIX_TIMESTAMP"))
  //构建以(用户，商户)分组时间戳排序的窗口提供开窗函数使用
  val windowPartitionByCardNumAndAcptorOrderByTimestamp = Window.partitionBy(col("SD_PAN"), col("SD_RETL_ID"))
    .orderBy(col("LOCAL_UNIX_TIMESTAMP"))

  //构建多个以用户分组时间戳排序限定时间范围的窗口提供开窗函数使用
  val timeGroupByCardNum = timeInterval.map {
    time => {
      val winSpec = windowPartitionByCardNumOrderByTimestamp.rangeBetween(-time * 60 * 60, -1)
      val timeStr = time match {
        case caseValue if (caseValue.>=(24)) => time./(24).toString().+("Day")
        case _ => time.toString().+("Hour")
      }
      (winSpec, timeStr)
    }
  }

  //构建多个以(用户, 商户)分组时间戳排序限定时间范围的窗口 提供开窗函数使用
  val timeGroupByAcptorAndCardNum = timeInterval.map {
    time => {
      val winSpec = windowPartitionByCardNumAndAcptorOrderByTimestamp.rangeBetween(-time * 60 * 60, -1)
      val timeStr = time match {
        case caseValue if (caseValue.>=(24)) => time./(24).toString().+("Days")
        case _ => time.toString().+("Hours")
      }
      (winSpec, timeStr)
    }
  }

  //以用户分组时间排序取当前向前几行数据
  val countRows = Array(
    1,
    7,
    15,
    30)

  //构建多个以用户分组时间戳排序限定行数方位的窗口提供开窗函数使用
  val rowsGroupByCardNum0 = countRows.map {
    numRows => {
      val winSpec = windowPartitionByCardNumOrderByTimestamp.rowsBetween(-numRows, 0)
      val timeStr = numRows.toString().+("Times")
      (winSpec, timeStr)
    }
  }
  val rowsGroupByCardNum1 = countRows.map {
    numRows => {
      val winSpec = windowPartitionByCardNumOrderByTimestamp.rowsBetween(-numRows, -1)
      val timeStr = numRows.toString().+("Times")
      (winSpec, timeStr)
    }
  }
  /* 构建多种情况的窗口
   * 当计算近N次时，需要不包括当前记录
   * 但是当计算近N次相同时，由于会用到当前记录
   * 所以一种是rowsBetween()的第二个参数取-1，一种是取0
   */
  val rowsGroupByCardNumFunc = (flag: Int) => {
    if (flag == 0) rowsGroupByCardNum0 else rowsGroupByCardNum1
  }

  //以用户分组时间戳排序限定当前行向前一月范围的窗口
  val windowSpec_oneMonth = windowPartitionByCardNumOrderByTimestamp.rangeBetween(-60 * 60 * 24 * 30, -1)

  /*
   * 抽取第一个月数据进行计算

  def accordingDateExtractData(inputPath: String, endDate:String, calcDay: Int) = {
    val (beginDate, effectDate) = UtilitySource.dateIndexWithInputDays(endDate, calcDay)

    val inputDf = sparksession.read.option("mergeSchema", true).parquet(inputPath)
                              .where(col("LOCAL_DATE").cast(DateType).between(beginDate, endDate))
                              .withColumn("MD_TRAN_AMT3", col("MD_TRAN_AMT3").cast(DoubleType))
    inputDf
  }*/
  /*
   * 开窗函数计算各列指标
   *
   * @param columns Seq[String] 要计算哪些列表达式
   * @param aggOpts Seq[String] 要计算的指标函数名
   * @param group Array[(WindowSpec, String)]几种开窗函数表达式
   * @return TraversableOnce[Column] 最终计算的表达式
   */
  def makeColumnByWindow(columns: Seq[Column],
                         aggOpts: Seq[String],
                         group: Array[(WindowSpec, String)]): TraversableOnce[Column] = {
    val cols = ArrayBuffer.empty[Column]

    //对每个开窗函数
    for (indices <- group) {
      //对每列名称
      for (elem <- columns) {
        val elem_name = elem.toString().trim().split("`")
        val name = if (elem_name.size.>(1)) elem_name.drop(1).mkString else elem_name.head

        //每个操作函数
        aggOpts.foreach { funName =>
          cols += callUDF(funName, elem).over(indices._1).as(s"${name}_${funName}_${indices._2}")
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
    array.map {
      columnExpr =>
        val elem_name = columnExpr.toString().trim().split("`")
        val name = if (elem_name.size.>(1)) elem_name.drop(1).mkString else elem_name.head
        sameValueTimesUDF(columnExpr).as(name + "_same_num")
    }
  }

  def continuousTrainVarCalc(inputDf: Dataset[Row], outputFile: String, saveMode: String) = {

    //------------------上边为定义的N小时和N次的时间窗口参数--------------------------------
    println("------signal 1------")

    // 原始金额字段分组，获取指定消费字段
    /**
     * 通过序列指定条件语句（下面序列大小为3内容如下），逻辑语句
     * MD_TRAN_AMT3
     * CASE WHEN (SD_TRAN_CDE_CAT = CA) THEN MD_TRAN_AMT3 END AS `Cash_AMT`
     * CASE WHEN (SD_TRAN_CDE_CAT = PU) THEN MD_TRAN_AMT3 END AS `Consume_AMT`
     */
    val basicAmtColumn = Seq(
      //总授权金额
      col("MD_TRAN_AMT3"),
      //取现金额
      when(col("SD_TRAN_CDE_CAT").equalTo("CA"), col("MD_TRAN_AMT3")).as("Cash_AMT"),
      //消费金额
      when(col("SD_TRAN_CDE_CAT").equalTo("PU"), col("MD_TRAN_AMT3")).as("Consume_AMT")
    )
    /**
     *
     * CASE WHEN (SD_ORIG_WIR_CNTRY_CDE = 156) THEN Consume_AMT END AS `china_amt`
     * CASE WHEN (NOT (SD_ORIG_WIR_CNTRY_CDE = 156)) THEN Consume_AMT END AS `abroad_amt`
     * CASE WHEN (SD_ORIG_WIR_CNTRY_CDE IN (840, 372, 250)) THEN Consume_AMT END AS `high_risk_country_a_amt`
     * CASE WHEN ((NOT (SD_ORIG_WIR_CNTRY_CDE = 156)) AND (SD_RETL_SIC_CDE IN (5735, 4121))) THEN Consume_AMT END AS `high_risk_mcc_code_a_amt`
     */
    val mTimesAmtCols = Seq(
      //国内商户消费金额
      when(col("SD_ORIG_WIR_CNTRY_CDE").equalTo("156"), col("Consume_AMT")).as("china_amt"),
      //国外商户授权金额
      when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"), col("Consume_AMT")).as("abroad_amt"),
      //近N小时高风险国家A授权金额指标
      when(col("SD_ORIG_WIR_CNTRY_CDE").isin("840", "372", "250"), col("Consume_AMT")).as("high_risk_country_a_amt"),
      //国外高风险MCC_CODE A 授权金额
      when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156") && col("SD_RETL_SIC_CDE").isin("5735", "4121"), col("Consume_AMT")).as("high_risk_mcc_code_a_amt")
    )

    val mTimesAmtColsStrs = Seq("china_amt", "abroad_amt", "high_risk_country_a_amt", "high_risk_mcc_code_a_amt")
    val basicMTimesAmtStrs = Seq("MD_TRAN_AMT3", "Cash_AMT", "Consume_AMT").++(mTimesAmtColsStrs)

    /*
     * 将原始数据按照小时进行聚合，计算每个特征的和，最大值，次数，
     */
    println("signal 2")
    val timeGroupColumns = ArrayBuffer.empty[Column]//创建数组，数组类型Column，处理完结果大小21
    basicMTimesAmtStrs.map {
      column => {
        Seq("max", "count", "sum").foreach {
          funcname => {
            timeGroupColumns.+=(callUDF(funcname, col(column)).as(s"${column}_${funcname}_Hour"))
          }
        }
      }
    }

    inputDf.
      select(Seq(col("*")).++(basicAmtColumn):_*)
      .select(Seq(col("*")).++(mTimesAmtCols):_*)
      .select((timeGroupColumns):_*)
      .show()

    //计算近N小时内的特征
    val columnsLastestHours = ArrayBuffer.empty[Column]

    Seq("max", "count", "sum").foreach {
      funcname => {
        columnsLastestHours.++=(makeColumnByWindow(basicMTimesAmtStrs.map {
          column => {
            col(s"${column}_${funcname}_Hour")
          }
        }, Seq(funcname), timeGroupByCardNum))
      }
    }

    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_sum_Hour")), Seq("stddev_pop"), timeGroupByCardNum))
    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_count_Hour")), Seq("count"), timeGroupByAcptorAndCardNum))
    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_sum_Hour")), Seq("sum"), timeGroupByAcptorAndCardNum))


    val divcolumns = ArrayBuffer.empty[Column]
    mTimesAmtColsStrs.foreach {
      column => {
        Seq(("1", "7"), ("7", "15")).foreach {
          days => {
            divcolumns += ((col(s"${column}_sum_Hour_sum_${days._1}Day") / col(s"${column}_count_Hour_count_${days._1}Day")) / (col(s"${column}_sum_Hour_sum_${days._2}Day") / col(s"${column}_count_Hour_count_${days._2}Day"))).as(s"${column}_avg_${days._1}Day_div_${days._2}Day")
          }
        }
      }
    }

    println("signal 3")

    //构造衍生数据计算方式
    val columns = ArrayBuffer.empty[Column]

    //求前一个月内时间最大最小时间开窗函数
    columns ++= makeColumnByWindow(Seq(col("LOCAL_TIME")), Seq("min", "max"), Array((windowSpec_oneMonth, "oneMonth")))

    //近N小时相同消费次数，相同城市次数，相同商户次数
    columns ++= sameValueCount(makeColumnByWindow(
      Seq(
        col("Consume_AMT"), col("SD_CITYNO"), col("SD_RETL_ID")
      ), Seq("collect_list"), timeGroupByCardNum
    ))

    //两次交易时间间隔LOCAL_UNIX_TIMESTAMP
    columns += (col("LOCAL_UNIX_TIMESTAMP") - lag(col("LOCAL_UNIX_TIMESTAMP"), 1).over(windowPartitionByCardNumOrderByTimestamp)).as("trans_time_interval")
    //上次交易国家代码
    columns += lag(col("SD_ORIG_WIR_CNTRY_CDE"), 1).over(windowPartitionByCardNumOrderByTimestamp).as("before_time_country")

    //时间排序近N次授权金额衍生特征
    columns ++= makeColumnByWindow(Seq(col("Consume_AMT")), Seq("max", "stddev_pop", "avg"), rowsGroupByCardNumFunc(1))

    columns ++= makeColumnByWindow(mTimesAmtCols, Seq("avg", "max"), rowsGroupByCardNumFunc(1))


    //近N次交易相同消费次数，城市次数，商户次数
    columns ++= sameValueCount(makeColumnByWindow(
      Seq(
        col("Consume_AMT"), col("SD_CITYNO"), col("SD_RETL_ID")
      ), Seq("collect_list"), rowsGroupByCardNumFunc(0)
    ))

    val columns2 = ArrayBuffer.empty[Column]

    //上次交易国家代码是否和本次相同
    columns2 += col("SD_ORIG_WIR_CNTRY_CDE").notEqual(col("before_time_country")).as("before_country_different_flag")

    //用户相邻两次交易是否同时在国内或同时在国外
    columns2 += (col("before_time_country").equalTo("156") and col("SD_ORIG_WIR_CNTRY_CDE").equalTo("156") ||
      (col("before_time_country").notEqual("156") and col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"))
      ).as("before_country_same_flag")
    println("signal 4")
    //计算过某些的指标进行除操作
    for (indices <- countRows) {
      columns2 += (col("Consume_AMT") / col(s"Consume_AMT_max_${indices}Times")).as(s"Consume_AMT_div_max_${indices}Times")
      columns2 += (col("Consume_AMT") / col(s"Consume_AMT_avg_${indices}Times")).as(s"Consume_AMT_div_avg_${indices}Times")
    }
    columns2 += ((col("Consume_AMT") - col(s"Consume_AMT_avg_30Times")) / col(s"Consume_AMT_stddev_pop_30Times")).as("Consume_AMT_avg_div_stddev_pop_30_Times_max")

    mTimesAmtColsStrs.foreach {
      column => {
        Seq(("1", "7"), ("7", "15")).foreach {
          days => {
            columns2 += (col(s"${column}_avg_${days._1}Times") / col(s"${column}_avg_${days._2}Times")).as(s"${column}_avg_${days._1}Times_div_${days._2}Times")
          }
        }
      }
    }
    //------------------------------------------------------------------------


  }


  def continuousTestVarCalc(prop: Properties, spark: SparkSession) = {
    //get properties
    val inputCalcFile = prop.getProperty("test_sample_calc")
    val mccFile = prop.getProperty("mcc_file")
    val saveMode = prop.getProperty("saveMode")
    val teststartdate = prop.getProperty("testcalcdate")
    val dropcolumn = prop.getProperty("droptestcolumn")
    val discretecolumns = prop.getProperty("discretecolumns").split(", ").map(_.trim())
    val discreteDefault = prop.getProperty("discreteDefault")

    val inputCalcDf = spark.read.option("mergeSchema", true).parquet(inputCalcFile)
      .withColumn("MD_TRAN_AMT3", col("MD_TRAN_AMT3").cast(DoubleType))

    val outputFile = inputCalcFile + "_var_output"

    //------------------上边为定义的N小时和N次的时间窗口参数--------------------------------
    println("signal 1")
    //添加mcc索引列
    val mccIndex = UtilitySource.mccCodeIndexClass(mccFile)

    val (dfInsertMccBigClassColumn, maxIndex) = UtilitySource.dfAddMccIndexColumn(inputCalcDf, mccIndex, "SD_RETL_SIC_CDE")

    val df = dfInsertMccBigClassColumn.na.fill(0.0, Seq("MD_TRAN_AMT3"))

    val columnsBasic = ArrayBuffer.empty[Column]
    //索取原始金额字段分组，获取指定消费字段
    val basicAmtColumn = Seq(
      //总授权金额
      col("MD_TRAN_AMT3"),
      //根据交易类别代码交易类别SD_TRAN_CDE_CAT  取现金额
      when(col("SD_TRAN_CDE_CAT").equalTo("CA"), col("MD_TRAN_AMT3")).as("Cash_AMT"),
      //消费金额
      when(col("SD_TRAN_CDE_CAT").equalTo("PU"), col("MD_TRAN_AMT3")).as("Consume_AMT")
    )

    val mTimesAmtCols = Seq(
      //国内商户消费金额  交易国家代码: SD_ORIG_WIR_CNTRY_CDE
      when(col("SD_ORIG_WIR_CNTRY_CDE").equalTo("156"), col("Consume_AMT")).as("china_amt"),
      //国外商户授权金额
      when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"), col("Consume_AMT")).as("abroad_amt"),
      //近N小时高风险国家A授权金额指标
      when(col("SD_ORIG_WIR_CNTRY_CDE").isin("840", "372", "250"), col("Consume_AMT")).as("high_risk_country_a_amt"),
      //国外高风险MCC_CODE A 授权金额
      when(col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156") && col("SD_RETL_SIC_CDE").isin("5735", "4121"), col("Consume_AMT")).as("high_risk_mcc_code_a_amt")
    )

    val mTimesAmtColsStrs = Seq("china_amt", "abroad_amt", "high_risk_country_a_amt", "high_risk_mcc_code_a_amt")
    val basicMTimesAmtStrs = Seq("MD_TRAN_AMT3", "Cash_AMT", "Consume_AMT").++(mTimesAmtColsStrs)
    /*
     * 将原始数据按照小时进行聚合，计算每个特征的和，最大值，次数，
     */
    println("signal 2")
    val timeGroupColumns = ArrayBuffer.empty[Column]
    basicMTimesAmtStrs.map {
      column => {
        Seq("max", "count", "sum").foreach {
          funcname => {
            timeGroupColumns.+=(callUDF(funcname, col(column)).as(s"${column}_${funcname}_Hour"))
          }
        }
      }
    }

    //计算近N小时内的特征
    val columnsLastestHours = ArrayBuffer.empty[Column]

    Seq("max", "count", "sum").foreach {
      funcname => {
        columnsLastestHours.++=(makeColumnByWindow(basicMTimesAmtStrs.map {
          column => {
            col(s"${column}_${funcname}_Hour")
          }
        }, Seq(funcname), timeGroupByCardNum))
      }
    }

    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_sum_Hour")), Seq("stddev_pop"), timeGroupByCardNum))
    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_count_Hour")), Seq("count"), timeGroupByAcptorAndCardNum))
    columnsLastestHours.++=(makeColumnByWindow(Seq(col("Consume_AMT_sum_Hour")), Seq("sum"), timeGroupByAcptorAndCardNum))

    val divcolumns = ArrayBuffer.empty[Column]
    mTimesAmtColsStrs.foreach {
      column => {
        Seq(("1", "7"), ("7", "15")).foreach {
          days => {
            divcolumns += ((col(s"${column}_sum_Hour_sum_${days._1}Day") / col(s"${column}_count_Hour_count_${days._1}Day")) / (col(s"${column}_sum_Hour_sum_${days._2}Day") / col(s"${column}_count_Hour_count_${days._2}Day"))).as(s"${column}_avg_${days._1}Day_div_${days._2}Day")
          }
        }
      }
    }

    println("signal 3")

    //构造衍生数据计算方式
    val columns = ArrayBuffer.empty[Column]

    //求前一个月内时间最大最小时间开窗函数
    columns ++= makeColumnByWindow(Seq(col("LOCAL_TIME")), Seq("min", "max"), Array((windowSpec_oneMonth, "oneMonth")))

    //近N小时相同消费次数，相同城市次数，相同商户次数
    columns ++= sameValueCount(makeColumnByWindow(
      Seq(
        col("Consume_AMT"), col("SD_CITYNO"), col("SD_RETL_ID")
      ), Seq("collect_list"), timeGroupByCardNum
    ))

    //两次交易时间间隔LOCAL_UNIX_TIMESTAMP
    columns += (col("LOCAL_UNIX_TIMESTAMP") - lag(col("LOCAL_UNIX_TIMESTAMP"), 1).over(windowPartitionByCardNumOrderByTimestamp)).as("trans_time_interval")
    //上次交易国家代码
    columns += lag(col("SD_ORIG_WIR_CNTRY_CDE"), 1).over(windowPartitionByCardNumOrderByTimestamp).as("before_time_country")

    //时间排序近N次授权金额衍生特征
    columns ++= makeColumnByWindow(Seq(col("Consume_AMT")), Seq("max", "stddev_pop", "avg"), rowsGroupByCardNumFunc(1))

    columns ++= makeColumnByWindow(mTimesAmtCols, Seq("avg", "max"), rowsGroupByCardNumFunc(1))


    //近N次交易相同消费次数，城市次数，商户次数
    columns ++= sameValueCount(makeColumnByWindow(
      Seq(
        col("Consume_AMT"), col("SD_CITYNO"), col("SD_RETL_ID")
      ), Seq("collect_list"), rowsGroupByCardNumFunc(0)
    ))

    val columns2 = ArrayBuffer.empty[Column]

    //上次交易国家代码是否和本次相同
    columns2 += col("SD_ORIG_WIR_CNTRY_CDE").notEqual(col("before_time_country")).as("before_country_different_flag")

    //用户相邻两次交易是否同时在国内或同时在国外
    columns2 += (col("before_time_country").equalTo("156") and col("SD_ORIG_WIR_CNTRY_CDE").equalTo("156") ||
      (col("before_time_country").notEqual("156") and col("SD_ORIG_WIR_CNTRY_CDE").notEqual("156"))
      ).as("before_country_same_flag")
    println("signal 4")
    //计算过某些的指标进行除操作
    for (indices <- countRows) {
      columns2 += (col("Consume_AMT") / col(s"Consume_AMT_max_${indices}Times")).as(s"Consume_AMT_div_max_${indices}Times")
      columns2 += (col("Consume_AMT") / col(s"Consume_AMT_avg_${indices}Times")).as(s"Consume_AMT_div_avg_${indices}Times")
    }
    columns2 += ((col("Consume_AMT") - col(s"Consume_AMT_avg_30Times")) / col(s"Consume_AMT_stddev_pop_30Times")).as("Consume_AMT_avg_div_stddev_pop_30_Times_max")

    mTimesAmtColsStrs.foreach {
      column => {
        Seq(("1", "7"), ("7", "15")).foreach {
          days => {
            columns2 += (col(s"${column}_avg_${days._1}Times") / col(s"${column}_avg_${days._2}Times")).as(s"${column}_avg_${days._1}Times_div_${days._2}Times")
          }
        }
      }
    }
    //------------------------------------------------------------------------
    println("signal 5")
    println(basicAmtColumn.drop(1))
    println(Seq(col("*")).++(basicAmtColumn.drop(1).++(mTimesAmtCols)))
    val dfBasic = df.select(Seq(col("*")).++(basicAmtColumn.drop(1)): _*)
      .select(Seq(col("*")).++(mTimesAmtCols): _*)
    println("dfBasic columns:" + dfBasic.columns.mkString(", "))
    println("signal 6")
    val dfTime = dfBasic.groupBy(col("SD_PAN"), col("SD_RETL_ID"), unix_timestamp(date_format(col("DD_DATE"), "yyyy-MM-dd HH")).as("LOCAL_UNIX_TIMESTAMP"))
      .agg(timeGroupColumns.head, timeGroupColumns.drop(1): _*)
      .select(Seq(col("*")).++(columnsLastestHours): _*)
      .select(Seq(col("*")).++(divcolumns): _*)
      .withColumn("timeId", concat_ws("_", col("SD_PAN"), col("LOCAL_UNIX_TIMESTAMP")))
      .drop("SD_PAN", "SD_RETL_ID", "LOCAL_UNIX_TIMESTAMP")
    println("dfTime columns: " + dfTime.columns.mkString(", "))
    println("signal 7")
    val dfTimes = dfBasic.withColumn("LOCAL_UNIX_TIMESTAMP", unix_timestamp(
      concat_ws(" ", col("LOCAL_DATE"), col("LOCAL_TIME")))
    )
      .select(Seq(col("*")).++(columns): _*)
      .select(Seq(col("*")).++(columns2): _*)
      .withColumn("timeId", concat_ws("_", col("SD_PAN"), unix_timestamp(date_format(col("DD_DATE"), "yyyy-MM-dd HH"))))
      .filter(col("LOCAL_DATE").>=(teststartdate) && col("MD_TRAN_AMT1").>=(0))

    println("dfTimes columns:" + dfTimes.columns.mkString(", "))

    println("signal 8")
    val fillMap = Map(
      "trans_time_interval" -> 60 * 60 * 24 * 30,
      "before_country_different_flag" -> false,
      "before_country_same_flag" -> false
    )
    //处理离散字段的缺失值
    val isNoneFunc = (str: String) => {
      if (str.equals("") || str.equals("null")) {
        discreteDefault
      } else {
        str
      }
    }
    spark.udf.register("isNone", isNoneFunc)
    val discolumns = ArrayBuffer.empty[Column]
    discretecolumns.foreach {
      column => {
        discolumns += callUDF("isNone", col(column)).as(column)
      }
    }
    println(fillMap)
    val dropcolumns = dropcolumn.split(",").map(_.trim())
    val lastcolumns = dfTimes.columns.diff(dfTime.columns).++(dfTime.columns).filterNot(dropcolumns.contains(_))
    println("lastcolumns:" + lastcolumns.mkString(", "))

    dfTimes.join(dfTime, dfTimes.col("timeId") === dfTime.col("timeId"), "left")
      .select(lastcolumns.diff(discretecolumns).map(col).++:(discolumns): _*)
      .na.fill(fillMap).na.fill(0.0)
      .repartition(1)
      .write
      .mode(saveMode)
      .option("header", true)
      .csv(outputFile)
  }

  /*
   * 计算20个训练集的特征
   */
  def calcTrainSetVar(prop: Properties, spark: SparkSession) {
    val inputFile = prop.getProperty("train_sample")
//    val mccFile = prop.getProperty("mcc_file")
    val saveMode = prop.getProperty("saveMode")
//    val dropcolumn = prop.getProperty("droptraincolumn")
//    val discretecolumns = prop.getProperty("discretecolumns").split(", ").map(_.trim())
//    val discreteDefault = prop.getProperty("discreteDefault")


    val inputDf = spark.read.option("mergeSchema", true)
      .parquet(inputFile + "/")
      .withColumn("MD_TRAN_AMT3", col("MD_TRAN_AMT3").cast(DoubleType))// withColumn 增加或者更新列
    val outputFile = inputFile + "/" + "result_output"
    continuousTrainVarCalc(inputDf, outputFile, saveMode)
  }


/*
 * 计算测试集的特征

def calcTestSetVar(prop: Properties, spark: SparkSession){

  val inputCalcFile = prop.getProperty("test_sample_calc")
  val mccFile = prop.getProperty("mcc_file")
  val saveMode = prop.getProperty("saveMode")
  val teststartdate = prop.getProperty("testcalcdate")
  val dropcolumn = prop.getProperty("droptestcolumn")
  val inputCalcDf = spark.read.option("mergeSchema", true).parquet(inputCalcFile)
                                .withColumn("MD_TRAN_AMT3", col("MD_TRAN_AMT3").cast(DoubleType))

  val outputFile = inputCalcFile + "_var_output"
  continuousTestVarCalc(inputCalcDf, mccFile, outputFile, saveMode, teststartdate, dropcolumn)
}
*
*/

def main (args: Array[String] ): Unit = {
  Logger.getLogger ("org").setLevel (Level.ERROR)

  val propFile = args (0)

  val prop = PropertiesUtil.loadProperties (propFile)
  val isLocal = prop.getProperty ("isLocal")
  val funcName = prop.getProperty ("funcName")
  println ("propFile: " + propFile + "\n" + "isLocal: " + isLocal + "\n" + "funcName: " + funcName)

  val sparkConf = SparkSession.builder ().appName (s"spark_calc_var_${
  funcName
}")

  isLocal match {
  case "true" => sparkConf.master ("local")
  case _ => sparkConf
}
  val spark = sparkConf.getOrCreate ()

  funcName match {
  case "train" => calcTrainSetVar (prop, spark)
  case "test" => continuousTestVarCalc (prop, spark)
}
}
}
