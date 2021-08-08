package com.netcloudai.bigdata.api.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc 演示DataStream-Source-自定义数据源-MySQL
 * API
 * 一般用于学习测试,模拟生成一些数据
 * Flink还提供了数据源接口,我们实现该接口就可以实现自定义数据源，不同的接口有不同的功能，分类如下：
 * SourceFunction:非并行数据源(并行度只能=1)
 * RichSourceFunction:多功能非并行数据源(并行度只能=1)
 * ParallelSourceFunction:并行数据源(并行度能够>=1)
 * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
 */
public class SourceDemo05_Customer_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> studentDS = env.addSource(new MySQLSource()).setParallelism(1);
        studentDS.print();
        env.execute();
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }
    public static class MySQLSource extends RichParallelSourceFunction<Student> {
        private boolean flag = true;
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        //open只执行一次,适合开启资源
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://172.16.240.103:3306/face", "root", "Sunmnet@123");
            String sql = "select id,name,age from warning";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
            while (flag) {
                rs = ps.executeQuery();//执行查询
                while (rs.next()) {
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    ctx.collect(new Student(id, name, age));
                }
                Thread.sleep(5000);
            }
        }

        //接收到cancel命令时取消数据生成
        @Override
        public void cancel() {
            flag = false;
        }

        //close里面关闭资源
        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        }
    }
}
