package com.netcloudai.bigdata.tabe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 需求
 * 使用SQL和Table两种方式对DataStream中的单词进行统计
 */
public class Demo02_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("World", 1),
                new WC("Hello", 1));
        //注册视图
        tEnv.createTemporaryView("WordCount", input, $("word"), $("frequency"));
        //执行sql
        Table resultTable = tEnv.sqlQuery("select word,sum(frequency) as frequency from WordCount group by word");
        //转为DataStream
        /**
         *toAppendStream(): 将计算后的数据 append到结果的DataStream中
         *toRetractStream(): 将计算后的新数据在DataStream原数据基础上更新 true或删除false
         * 4> (true,FlinkSQL_Table_Demo02.WC(word=World, frequency=1))
         * 7> (true,FlinkSQL_Table_Demo02.WC(word=Hello, frequency=1))
         * 7> (false,FlinkSQL_Table_Demo02.WC(word=Hello, frequency=1))
         * 7> (true,FlinkSQL_Table_Demo02.WC(word=Hello, frequency=2))
         */
        DataStream<Tuple2<Boolean, WC>> resultDS = tEnv.toRetractStream(resultTable, WC.class);
        resultDS.print();

        //注册表
        Table table = tEnv.fromDataStream(input);
        //执行查询
        Table resultTable2 = table
                .groupBy($("word"))
                .select($("word"), $("frequency").sum().as("frequency"))
                .filter($("frequency").isEqual(2));
        DataStream<Tuple2<Boolean, WC>> resultDS2 = tEnv.toRetractStream(resultTable2, WC.class);
        resultDS2.print();
        /**
         * 7> (true,FlinkSQL_Table_Demo02.WC(word=Hello, frequency=2))
         */
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        public long frequency;
    }
}
