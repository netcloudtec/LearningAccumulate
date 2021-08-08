package com.netcloudai.bigdata.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/8/7 21:12
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 2 从文件中读取数据加载为DataSet
        DataSet<String> inputDataSet = env.readTextFile("data/wc/hello.txt");

        // TODO 3 将读取的每行数据根据指定分隔符拆分为单词 返回一个Tuple2二元组
        /**
         * 匿名内部类注意输入输出数据的泛型定义。
         * FlatMapOperator<String, Tuple2<String, Integer>> flatMapDataSet
         * 泛型 String表示输入的数据类型
         * 泛型 Tuple2<String, Integer> 表示输出的数据类型
          */
        FlatMapOperator<String, Tuple2<String, Integer>> flatMapDataSet = inputDataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    // 这里的out是数据收集器，表示将处理的数据收集起来。
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //TODO 4 进行单词统计 groupBy(0) 这里的0表示Tuple的第一个位置；sum(1) 表示元组的第二个位置
        DataSet<Tuple2<String, Integer>> resultSet = flatMapDataSet.groupBy(0).sum(1);
        //TODO 5 结果输出打印
        resultSet.print();
    }
}
