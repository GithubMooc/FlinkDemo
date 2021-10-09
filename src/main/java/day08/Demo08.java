package day08;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 18:51
 * @Name 使用SQL查询
 */
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Tuple2<String, String>> stream = executionEnvironment
                .fromElements(
                        Tuple2.of("Mary", "./home"),
                        Tuple2.of("Bob", "./cart"),
                        Tuple2.of("Mary", "./prod?id=1"),
                        Tuple2.of("Liz", "./home"));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);
        Table table = tableEnvironment.fromDataStream(stream, $("f0").as("user"), $("f1").as("url"));

        //注册临时视图
        tableEnvironment.createTemporaryView("clicks",table);

        Table result = tableEnvironment.sqlQuery("SELECT user, COUNT(url) FROM clicks GROUP BY user");

//        查询结果转换成数据流
//        更新日志流（用于查询中有聚合操作的情况）
        tableEnvironment.toChangelogStream(result).print();
        executionEnvironment.execute();
    }

}
