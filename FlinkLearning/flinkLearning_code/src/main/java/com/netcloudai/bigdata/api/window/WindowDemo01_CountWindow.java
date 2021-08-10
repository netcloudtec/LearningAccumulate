package com.netcloudai.bigdata.api.window;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 计数窗口比较难理解
 *
 */
public class WindowDemo01_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<CartInfo> cartInfoDS = socketDS.map(new MapFunction<String, CartInfo>() {
            public CartInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new CartInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });
        //需求1:统计在最近的消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计--基于数量的滚动窗口
        // 不要理解每次接收5条消息，然后去统计最近的5条消息各个key的和
        SingleOutputStreamOperator<CartInfo> result1 = cartInfoDS.keyBy(CartInfo::getSensorId)
                .countWindow(5)
                .sum("count");
        result1.print();
        //需求2:统计在最近消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计最近相同key5次的和--基于数量的滑动窗口
        SingleOutputStreamOperator<CartInfo> result2 = cartInfoDS.keyBy(CartInfo::getSensorId)
                .countWindow(5, 3)
                .sum("count");
        result2.print();
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        private String sensorId;//信号灯id
        private Integer count;//通过该信号灯的车的数量
    }
}
