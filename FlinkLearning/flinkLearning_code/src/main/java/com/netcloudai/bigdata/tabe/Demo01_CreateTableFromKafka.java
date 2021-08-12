package com.netcloudai.bigdata.tabe;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
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
 *
 * 测试时，在本地环境中启动kafaka服务、创建主题、生产数据
 * 1、//查看所有主题
 * ./kafka-topics.sh --list --zookeeper bigdata01:2181
 * 2、创建主题topic
 * ./kafka-topics.sh --create --zookeeper bigdata01:2181 --replication-factor 1 --partitions 1 --topic test
 * 3、查看topic详情
 * ./kafka-topics.sh --describe --zookeeper bigdata01:2181 --topic test
 * 不跟topic则查看所以topic详情
 * ./kafka-topics.sh --describe --zookeeper bigdata01:2181
 * 4、删除topic
 * ./kafka-topics.sh --delete --zookeeper bigdata01:2181 --topic test
 * 5、消费者
 * ./kafka-console-consumer.sh --bootstrap-server bigdata01:6667 --topic test
 * 6、生产者
 * ./kafka-console-producer.sh --broker-list bigdata01:6667 --topic test
 *
 */
public class Demo01_CreateTableFromKafka {
    public static void main(String[] args) throws Exception {
        //1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                        "  'properties.bootstrap.servers' = 'bigdata01:6667',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")");
        String sql = "select " +
                "user_id," +
                "page_id," +
                "status " +
                "from input_kafka ";
        Table ResultTable = tEnv.sqlQuery(sql);
        DataStream resultDS = tEnv.toAppendStream(ResultTable, Row.class);//将Table转为DataStream
        resultDS.print();
        //3、sink To Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("bigdata01").setPort(6379).setDatabase(5).build();
        resultDS.addSink(new RedisSink<Row>(conf, new RedisExampleMapper()));
        //env.execute("kafkatoredis");


        tEnv.executeSql("CREATE TABLE hTable (\n" +
                " rowkey INT,\n" +
                " info ROW<page_id STRING,status STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-1.4',\n" +
                " 'table-name' = 'mytable',\n" +
                " 'zookeeper.quorum' = 'bigdata01:2181'\n" +
                ")");

        tEnv.executeSql("INSERT INTO hTable SELECT user_id, ROW(page_id,status) FROM input_kafka");
        tEnv.executeSql("select * from hTable").print();
//        resultDS.print();
        env.execute("kafkatoredis");
    }

    //指定Redis key并将flink数据类型映射到Redis数据类型
    public static final class RedisExampleMapper implements RedisMapper<Row> {
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "test");
        }

        @Override
        public String getKeyFromData(Row row) {
            return row.toString().split(",")[0];
        }

        @Override
        public String getValueFromData(Row row) {
            return row.toString();
        }
    }

}
