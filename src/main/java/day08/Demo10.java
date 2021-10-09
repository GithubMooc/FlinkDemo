package day08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pojo.UserBehavior;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 19:22
 * @Name FlinkSQL实现取前三名
 */
public class Demo10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = executionEnvironment
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                }).filter(t -> t.getBehavior().equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner((userBehavior, l) -> userBehavior.getTimestamp()));
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);

        Table table = streamTableEnvironment.fromDataStream(stream, $("userId"), $("itemId"), $("categoryId"), $("behavior"), $("timestamp").rowtime().as("ts"));

streamTableEnvironment.createTemporaryView("userbehavior",table);
        String s = "SELECT itemId, COUNT(itemId) as cnt, HOP_END(ts,INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd FROM userbehavior GROUP BY itemId, HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)";
//        按照窗口结束时间分区，然后按照浏览量降序排列
        String midSQL = "SELECT *, ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY cnt DESC) as row_num FROM (" + s + ")";
//        取出前三名
        String outerSQL = "SELECT * FROM (" + midSQL + ") WHERE row_num <= 3";
        Table itemViewCount = streamTableEnvironment.sqlQuery(outerSQL);
        streamTableEnvironment.toChangelogStream(itemViewCount).print();
        executionEnvironment.execute();
    }
}
