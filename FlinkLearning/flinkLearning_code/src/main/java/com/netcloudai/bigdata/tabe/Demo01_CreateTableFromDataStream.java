package com.netcloudai.bigdata.tabe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 需求
 * 将DataStream注册为Table和View并进行SQL统计
 * 编程步骤：
 * 1、准备环境 (创建 TableEnvironment)
 * 2、Source
 * 3、转换操作： 转换 DataStream to Table 或者为View 然后进行查询
 * 4、sink： 输出结果 将 Table 转为 DataStream
 */
public class Demo01_CreateTableFromDataStream {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        //2.Source
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));
        //3.转换操作
        // 转换 DataStream to Table 可以指定字段
        Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
        // 转换 DataStream as View
        tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));
        //Table API：执行查询
        Table resultTable1 = tableA.select($("user"),$("product"),$("amount"));

        //SQL语法： union the two tables
        Table resultTable = tEnv.sqlQuery(
                "SELECT * FROM " + tableA + " WHERE amount > 2 " +
                        "UNION ALL " +
                        "SELECT * FROM OrderB WHERE amount < 2"
        );

        //4.sink 输出结果 将 Table 转为 DataStream
        /**
         * 将 Table 转换为 DataStream 有两种模式：
         * Append Mode: 仅当动态 Table 仅通过INSERT更改进行修改时，才可以使用此模式，即，它仅是追加操作，并且之前输出的结果永远不会更新。
         * Retract Mode: 任何情形都可以使用此模式。它使用 boolean 值对 INSERT 和 DELETE 操作的数据进行标记。
         */
        DataStream<Order> resultDS1 = tEnv.toAppendStream(resultTable1, Order.class);
        DataStream<Tuple2<Boolean, Order>> resultDS = tEnv.toRetractStream(resultTable, Order.class);
//        resultDS.print();
//        resultDS1.print();
        System.out.println(tEnv.getCurrentCatalog());
        System.out.println(tEnv.getCurrentDatabase());
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }
}
