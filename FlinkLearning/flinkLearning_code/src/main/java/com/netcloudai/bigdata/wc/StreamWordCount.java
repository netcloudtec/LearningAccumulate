package com.netcloudai.bigdata.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/8/7 21:57
 * Flink流式计算是有状态的计算，保留历史结果值
 * 程序在没有指定并行度情况下，使用本地集群的CPU CORE作为默认并行度（我的机器是16Core）
 * 可以在程序环境中指定并行度 env.set
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //TODO 2 source: 从socket文本流读取数据 加载为DataStream
        DataStream<String> inputDataStream = env.socketTextStream(host, port);
        //TODO 3 transformation:基于数据流进行转换计算 每行数据根据指定分隔符拆分为单词 返回一个Tuple2二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //TODO 4 进行单词统计 key(0) 这里的0表示Tuple的第一个位置；sum(1) 表示元组的第二个位置
        DataStream<Tuple2<String, Integer>> resultStream = flatMapStream.keyBy(0).sum(1);
       //TODO 5 sink
        resultStream.print();
        //TODO 6 execute/启动并等待程序结束
        env.execute();
    }
}
