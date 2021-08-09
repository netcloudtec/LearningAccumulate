package com.netcloudai.bigdata.api.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc 演示DataStream-Sink-基于控制台和文件
 *
 */
public class SinkDemo01_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("localhost", 9999);
        ds.print();
        ds.print("输出标识");
        ds.printToErr();//会在控制台上以红色输出
        ds.printToErr("输出标识");//会在控制台上以红色输出
        ds.writeAsText("data/api/result1", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        ds.writeAsText("data/api/result2",FileSystem.WriteMode.OVERWRITE).setParallelism(2);
//        ds.writeAsCsv("file:///data/api/result3");
        env.execute();
    }
}
