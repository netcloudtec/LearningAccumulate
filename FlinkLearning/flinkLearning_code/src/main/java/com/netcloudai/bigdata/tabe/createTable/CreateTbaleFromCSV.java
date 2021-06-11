package com.netcloudai.bigdata.tabe.createTable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/4/15 15:55
 * @Desc 通过外部文件创建一个表 这里我们使用csv格式的文件去创建表（其他格式的文件参考官网）
 * 版本1.12已经弃用下面的创建方式：
 * tableEnv
 * .connect(...) // 定义表的数据来源，和外部系统建立连接
 * .withFormat(...) // 定义数据格式化方法
 * .withSchema(...) // 定义表结构
 * .createTemporaryTable("MyTable"); // 创建临时表
 */
public class CreateTbaleFromCSV {
    public static void main(String[] args) {
        //TODO 1、创建程序执行环境
        //创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);//基于流环境创建表环境（1.12版本默认使用blink）

        //TODO 2、指定Source 版本1.12创建表的方式 注意pom文件引入csv依赖
        tableEnv.executeSql("CREATE TABLE fs_table (\n" +
                "  user_id STRING,\n" +
                "  order_amount DOUBLE\n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='/Users/yangshaojun/Bigdata_AI/bigdata_workspace/flink_learning/data/test.csv',\n" +
                "  'format'='csv'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE out_table (\n" +
                "  user_id STRING,\n" +
                "  order_amount DOUBLE\n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='/Users/yangshaojun/Bigdata_AI/bigdata_workspace/flink_learning/data/test2.csv',\n" +
                "  'format'='csv'\n" +
                ")");
        //TODO 3、Transform操作
        Table tableResult = tableEnv.sqlQuery("select * from fs_table");
        Table select = tableEnv.from("fs_table").select($("user_id"), $("order_amount"));
        //TODO 4、Sink 表的输出
        tableResult.executeInsert("out_table");//将处理的结果输出到out_table表 executeInsert的执行，不需要再去执行 env.execute()
        /**
         *  将Table对象转为Stream 输出,这时必须执行 env.execute();
         */
        //tableEnv.toAppendStream(select,Row.class).print();
        //env.execute();
    }
}
