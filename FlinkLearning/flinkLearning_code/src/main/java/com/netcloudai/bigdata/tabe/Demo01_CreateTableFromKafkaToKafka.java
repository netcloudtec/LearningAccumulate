package com.netcloudai.bigdata.tabe;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Demo01_CreateTableFromKafkaToKafka {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //2.Source
        TableResult inputTable = tEnv.executeSql(
                "CREATE TABLE input_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'input_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")"
        );
        TableResult outputTable = tEnv.executeSql(
                "CREATE TABLE output_kafka (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `page_id` BIGINT,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'output_kafka',\n" +
                        "  'properties.bootstrap.servers' = 'node1:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'sink.partitioner' = 'round-robin'\n" +
                        ")"
        );

        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka " +
                "where status = 'success'";
        Table ResultTable = tEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> resultDS = tEnv.toRetractStream(ResultTable, Row.class);
        resultDS.print();

        tEnv.executeSql("insert into output_kafka select * from " + ResultTable);

        //7.excute
        env.execute();
    }


}

