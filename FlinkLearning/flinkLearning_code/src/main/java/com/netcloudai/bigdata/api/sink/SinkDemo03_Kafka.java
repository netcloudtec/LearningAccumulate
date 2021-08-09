package com.netcloudai.bigdata.api.sink;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.Charset;
import java.util.Properties;

/**
 * 实时处理结果存储到 Kafka中 
 */
public class SinkDemo03_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        DataStream<String> lines = env.socketTextStream("localhost", 9999);
        //写入Kafka的topic
        String topic = "test";
        //设置Kafka相关参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "netcloud01:9092,netcloud02:9092,netcloud03:9092");
        //创建FlinkKafkaProducer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                topic, //指定topic
                new KafkaStringSerializationSchema(topic), //指定写入Kafka的序列化Schema
                properties, //指定Kafka的相关参数
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE //指定写入Kafka为EXACTLY_ONCE语义
        );
        //添加KafkaSink
        lines.addSink(kafkaProducer);
        env.execute();
    }

    public static class KafkaStringSerializationSchema implements KafkaSerializationSchema<String> {
        private String topic;
        private String charset;

        //构造方法传入要写入的topic和字符集，默认使用UTF-8
        public KafkaStringSerializationSchema(String topic) {
            this(topic, "UTF-8");
        }

        public KafkaStringSerializationSchema(String topic, String charset) {
            this.topic = topic;
            this.charset = charset;
        }

        //调用该方法将数据进行序列化
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String element, @Nullable Long timestamp) {
            //将数据转成bytes数组
            byte[] bytes = element.getBytes(Charset.forName(charset));
            //返回ProducerRecord
            return new ProducerRecord<>(topic, bytes);
        }
    }
}
