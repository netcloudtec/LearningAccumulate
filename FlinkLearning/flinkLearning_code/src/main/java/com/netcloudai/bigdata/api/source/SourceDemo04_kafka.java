package com.netcloudai.bigdata.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Desc 演示DataStream-Source-基于 kafka
 */
public class SourceDemo04_kafka {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 1.source-kafka-test主题
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "bigdata01:6667,bigdata02:6667,bigdata03:6667");//集群地址 默认9092端口被修改了
        props.setProperty("group.id", "flink");//设置消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true"); //自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        //在这里要注意： 如果kafka只接收数据，从来没来消费过，程序一开始不要用latest，不然以前的数据就接收不到了。应当先earliest，然后二都都可以 。
        //1)代码端先earliest，最早提交的数据是可以获取到的，再生产数据也是可以获取到的。
        //2)将auto.offset.reset设置成latest，再生产数据也是可以获取到的。
        //3)虽然auto.offset.reset默认是latest，但是建议使用earliest。
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        //创建FlinkKafkaConsumer并传入相关参数
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "test2", //要读取数据的Topic名称
                new SimpleStringSchema(), //读取文件的反序列化Schema
                props //传入Kafka的参数
        );
        //使用addSource添加kafkaConsumer
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);
        /**
         * 注意：目前这种方式无法保证Exactly Once，Flink的Source消费完数据后，将偏移量定期的写入到Kafka的一个特殊的topic中，
         * 这个topic就是__consumer_offset，这种方式虽然可以记录偏移量，但是无法保证Exactly Once，后面学完了State后，再实现Exactly Once功能。
         */
        //TODO 2.transformation

        //TODO 3.sink
        lines.print();
        //TODO 4.execute
        env.execute();
    }
}
