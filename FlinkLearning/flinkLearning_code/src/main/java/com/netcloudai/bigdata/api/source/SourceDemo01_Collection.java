package com.netcloudai.bigdata.api.source;

import com.netcloudai.bigdata.api.bean.SensorReading;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Desc 演示DataStream-Source-基于集合
 * 有限的数据流程序执行完后就会退出
 */
public class SourceDemo01_Collection {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //TODO 1.source
        //非并行的Source
        //从集合中读取数据
        DataStream<SensorReading> ds1 = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        DataStream<Integer> ds2 = env.fromElements(1, 2, 4, 67, 189);

        //并行的Source
        DataStream<Long> ds3 = env.fromSequence(1, 100);
        //TODO 2.transformation
        System.out.println("ds1并行度：" + ds1.getParallelism());//1
        System.out.println("ds2并行度：" + ds2.getParallelism());//1
        System.out.println("ds4并行度：" + ds3.getParallelism());//8
        //TODO 3.sink
        ds1.print();
        ds2.print();
        ds3.print();
        //TODO 4.execute
        env.execute();
    }
}
