package com.netcloudai.bigdata.api.window;

import com.netcloudai.bigdata.api.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/8/10 01:18
 */
public class WindowDemo05_OtherAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试
        // 3. 其它可选API
        // 侧输出流，存储迟到的数据
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .trigger()
//                .evictor()
                //允许迟到的时间1min 时间在9点的时候触发计算，但是不关闭窗口，直到9点零1关闭 在这1min时间过程中 会继续更新计算窗口数据
                .allowedLateness(Time.minutes(1))// 迟到的1min数据每接收一条数据，就会更新之前处理的结果
                .sideOutputLateData(outputTag)//允许迟到的时间1min  结果发现1min太小还是有迟到的数据，存储到侧输出流中
                .sum("temperature");

        // 获取迟到的数据 单独进行数据处理（批处理）
        sumStream.getSideOutput(outputTag).print("late");

        sumStream.print();
        env.execute();
    }
}
