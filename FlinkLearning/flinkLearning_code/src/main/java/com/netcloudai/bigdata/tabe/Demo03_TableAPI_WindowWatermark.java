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
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Demo03_TableAPI_WindowWatermark {
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
        DataStream<Order> watermakerDS = orderDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.getCreateTime())
        );

        //4.注册表
        tEnv.createTemporaryView("t_order", watermakerDS,
                $("orderId"), $("userId"), $("money"), $("createTime").rowtime());

        //查看表约束
        tEnv.from("t_order").printSchema();

        //5.TableAPI查询
        Table ResultTable = tEnv.from("t_order")
                //.window(Tumble.over("5.second").on("createTime").as("tumbleWindow"))
                .window(Tumble.over(lit(5).second())
                        .on($("createTime"))
                        .as("tumbleWindow"))
                .groupBy($("tumbleWindow"), $("userId"))
                .select(
                        $("userId"),
                        $("userId").count().as("totalCount"),
                        $("money").max().as("maxMoney"),
                        $("money").min().as("minMoney"));


        //6.将SQL的执行结果转换成DataStream再打印出来
        DataStream<Tuple2<Boolean, Row>> resultDS = tEnv.toRetractStream(ResultTable, Row.class);
        resultDS.print();
        //7.excute
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
