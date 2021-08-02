package com.netcloud.bigdata.sparkstreaming


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * 默认kafka访问端口号 9092
 * 现在使用的端口号已经被修改了
 * root@bigdata02:/usr/hdp/3.1.0.0-78/kafka/conf# cat server.properties
 * port=6667
 * Kafka 常用命令
 * 1）查看主题列表
 * root@bigdata02:/usr/hdp/3.1.0.0-78/kafka/bin#
 * ./kafka-topics.sh --list --zookeeper bigdata01:2181
 * 2）创建主题
 * ./kafka-topics.sh --create --zookeeper bigdata01:2181 --replication-factor 1 --partitions 1 --topic ysjtest
 * 3）生产数据
 * ./kafka-console-producer.sh --broker-list bigdata01:6667 --topic ysjtest
 * 4) 消费数据
 * ./kafka-console-consumer.sh --bootstrap-server bigdata01:9001 --topic ysjtest
 * 5）删除主题
 * ./kafka-topics.sh --delete --zookeeper bigdata01:2181 --topic ysjtest
 * 6）查看主题详情
 * ./kafka-topics.sh --describe --zookeeper bigdata01:2181 --topic ysjtest
 * 7) 查看消费进度
 * ./kafka-consumer-groups.sh --describe --bootstrap-server bigdata01:6667 --group weblog
 *
 */
object SparkStreaming04_Kafka {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata01:6667,bigdata02:6667,bigdata03:6667",
            ConsumerConfig.GROUP_ID_CONFIG -> "weblog",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("ysjtest"), kafkaPara)
        )
        kafkaDataDS.map(_.value()).print()
        ssc.start()
        ssc.awaitTermination()
    }

}
