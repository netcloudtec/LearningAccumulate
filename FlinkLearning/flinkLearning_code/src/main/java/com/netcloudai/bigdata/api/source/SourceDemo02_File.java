package com.netcloudai.bigdata.api.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc 演示DataStream-Source-基于本地/HDFS的文件/文件夹/压缩文件
 * 有限的数据流程序执行完后就会退出
 */
public class SourceDemo02_File {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 1.source
        DataStream<String> ds1 = env.readTextFile("data/wc/sensor.txt");
        //TODO 2.transformation

        //TODO 3.sink
        ds1.print();
        //TODO 4.execute
        env.execute();
    }
}
