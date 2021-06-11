package com.netcloudai.bigdata.tabe.createTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/4/15 16:35
 * @Desc FlinkSQL直接消费Kafak数据 然后创建表
 * format：csv,json格式 前提是pom文件引入依赖
 * kafka生产者提供的数据格式：
 * json: {"user_id":123,"page_id":10,"status":"success"}
 * csv: 1,2,success
 * 测试时，在本地环境中启动kafaka服务、创建主题、生产数据
 */
public class CreateTbaleFromKafka {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //2.Source
        tEnv.executeSql(
                "CREATE TABLE input_kafka (\n" +
                        "  `user_id` INT,\n" +
                        "  `page_id` STRING,\n" +
                        "  `status` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'csv'\n" +
                        ")");
        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka ";
        Table ResultTable = tEnv.sqlQuery(sql);
        DataStream resultDS = tEnv.toAppendStream(ResultTable, Row.class);
        //3、sink To Redis
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).setDatabase(2).build();
//        resultDS.addSink(new RedisSink<Row>(conf, new RedisExampleMapper()));

        tEnv.executeSql("CREATE TABLE hTable (\n" +
                " rowkey INT,\n" +
                " info ROW<page_id STRING,status STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'mytable',\n" +
                " 'zookeeper.quorum' = '10.238.251.4:2181'\n" +
                ")");

        tEnv.executeSql("INSERT INTO hTable SELECT user_id, ROW(page_id,status) FROM input_kafka");
        tEnv.executeSql("select * from hTable").print();
        resultDS.print();
//        env.execute("kafkatoredis");
    }

//    //指定Redis key并将flink数据类型映射到Redis数据类型
//    public static final class RedisExampleMapper implements RedisMapper<Row> {
//        public RedisCommandDescription getCommandDescription() {
//            return new RedisCommandDescription(RedisCommand.HSET, "opc");
//        }
//
//        @Override
//        public String getKeyFromData(Row row) {
//            return row.toString().split(",")[0];
//        }
//
//        @Override
//        public String getValueFromData(Row row) {
//            return row.toString();
//        }


}
