package com.netcloudai.bigdata.api.window;
import com.netcloudai.bigdata.api.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 增量聚合函数 应用在计数窗口中
 */
public class WindowDemo01_CountWindowAggre {
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

        // 开计数窗口测试
        SingleOutputStreamOperator<Double> avgTempResultStream = dataStream.keyBy(SensorReading::getId)
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        avgTempResultStream.print();

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>{
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);//第一个元素是总和，第二个元素是相同key的个数
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            // 新来的数据值和原来的聚合数据相加
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            //返回的结果 平均值
            return accumulator.f0 / accumulator.f1;
        }
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            // 分区数据merge
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
