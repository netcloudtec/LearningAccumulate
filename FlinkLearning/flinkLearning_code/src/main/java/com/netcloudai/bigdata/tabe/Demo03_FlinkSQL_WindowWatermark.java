package com.netcloudai.bigdata.tabe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 需求
 * 使用Flink SQL来统计5秒内 每个用户的 订单总数、订单的最大金额、订单的最小金额
 * 也就是每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * 上面的需求使用流处理的Window的基于时间的滚动窗口就可以搞定!
 * 那么接下来使用FlinkTable&SQL-API来实现
 * <p>
 * 要求：使用事件时间 + WaterMark +FlinkSQL和Table中的window来实现
 */
public class Demo03_FlinkSQL_WindowWatermark {
    public static void main(String[] args) throws Exception {

        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //2.Source
        DataStreamSource<Order> orderDS = env.addSource(new RichSourceFunction<Order>() {
            private Boolean isRunning = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (isRunning) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        //3.Transformation
        /**
         *事件时间 + WaterMark
         */
        DataStream<Order> watermakerDS = orderDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((order, timestamp) -> order.getCreateTime())
                );
        //4.注册表 rowtime() :作用就是指定那一列是时间列
        tEnv.createTemporaryView("t_order", watermakerDS, $("orderId"), $("userId"), $("money"), $("createTime").rowtime());
        //5.执行SQL
        String sql = "select " +
                "userId," +
                "count(*) as totalCount," +
                "max(money) as maxMoney," +
                "min(money) as minMoney " +
                "from t_order " +
                "group by userId," +
                "tumble(createTime, interval '5' second)";

        Table ResultTable = tEnv.sqlQuery(sql);
        //6.Sink
        //将SQL的执行结果转换成DataStream再打印出来
        //toAppendStream → 将计算后的数据append到结果DataStream中去
        //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        DataStream<Tuple2<Boolean, Row>> resultDS = tEnv.toRetractStream(ResultTable, Row.class);
        resultDS.print();
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }
}

