package day08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 01:22
 * @Name FlinkSQL 流转为动态表
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream = executionEnvironment
                .fromElements(
                        Tuple3.of("Mary", "./home", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Bob", "./cart", 12 * 60 * 60 * 1000L),
                        Tuple3.of("Mary", "./prod?id=1", 12 * 60 * 60 * 1000L + 5 * 1000L),
                        Tuple3.of("Liz", "./home", 12 * 60 * 60 * 1000L + 60 * 1000L),
                        Tuple3.of("Bob", "./prod?id=3", 12 * 60 * 60 * 1000L + 90 * 1000L),
                        Tuple3.of("Mary", "./prod?id=7", 12 * 60 * 60 * 1000L + 105 * 1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(((stringStringLongTuple3, l) -> stringStringLongTuple3.f2)));

        EnvironmentSettings setting = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, setting);
//        rowtime()方法指定f2字段是事件时间
        Table table = tableEnvironment.fromDataStream(stream, $("f0").as("user"), $("f1").as("url"), $("f2").rowtime().as("cTime"));
//        数据流->动态表
        tableEnvironment.toDataStream(table).print();
        executionEnvironment.execute();
    }
}
