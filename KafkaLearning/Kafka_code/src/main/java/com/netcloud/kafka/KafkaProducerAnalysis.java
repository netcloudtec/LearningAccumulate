package com.netcloud.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/5/7 10:43
 * 消息的发送方式
 * 1、同步发送 send()方法发送完后调用 get()方法
 * 2、异步发送 send()方法直接发送
 * 3、有返回值发送： 在 send（）方法里指定一个 Callback 的回调函数，
 */
public class KafkaProducerAnalysis {
    public static final String brokerList = "10.238.251.4:6667";
    public static final String topic = "topic-demo";

    public static Properties initConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);//props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //重试次数
        props.put("retries", 10);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                StringSerializer.class.getName());
        return props;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = initConfig();
        //1、创建生产者实例
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            //2、ProducerRecord 构建消息
            //3、send()：异步发送 发送消息
            producer.send(new ProducerRecord<String, String>(topic,i+","+i));
        }

//        for (int i = 0; i < 100; i++) {
//            //2、ProducerRecord 构建消息
//            //3、send()：同步发送 发送消息
//            producer.send(new ProducerRecord<String, String>(topic, 1, Integer.toString(i), Integer.toString(i))).get();
//        }

//        for (int i = 0; i < 100; i++) {
//            //2、ProducerRecord 构建消息
//            //3、send()：异步发送 有返回值
//            producer.send(new ProducerRecord<String, String>(topic,Integer.toString(i)), new Callback() {
//                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
//                    if (exception != null) {
//                        exception.printStackTrace();
//                    } else {
//                        System.out.println(recordMetadata.topic() + "-" +
//                                recordMetadata.partition() + ":" + recordMetadata.offset());
//                    }
//                }
//            });
//        }

        producer.close();
    }
}
