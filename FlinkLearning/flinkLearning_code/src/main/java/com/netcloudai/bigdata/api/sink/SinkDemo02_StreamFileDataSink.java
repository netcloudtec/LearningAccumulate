package com.netcloudai.bigdata.api.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.net.URI;

/**
 * StreamingFileSink 替换原来的writeAsText 旧的API
 */
public class SinkDemo02_StreamFileDataSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStream<String> dataSteam = env.socketTextStream("localhost", 9999);

        DefaultRollingPolicy<String, String> rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(30 * 1000L) //30秒滚动生成一个文件
                .withMaxPartSize(1024L * 1024L * 100L) //当文件达到100m滚动生成一个文件 满足其中一个条件就会生产一个文件
                .build();

        //创建StreamingFileSink，数据以行格式写入
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path(new URI("data/api/result3")), //指的文件存储目录
                new SimpleStringEncoder<String>("UTF-8")) //指的文件的编码
                .withRollingPolicy(rollingPolicy) //传入文件滚动生成策略
                .build();
        //调用DataStream的addSink添加该Sink
        dataSteam.addSink(sink);
        env.execute();
    }
}
