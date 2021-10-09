package day08;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.UserBehavior;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Author Master
 * @Date 2021/10/8
 * @Time 19:21
 * @Name 统计UV，独立访客 unique visit
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                }).filter(t -> t.getBehavior().equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                        return userBehavior.getTimestamp();
                    }
                })).keyBy(r -> true).window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, HashSet<String>, Long>() {
                    @Override
                    public HashSet<String> createAccumulator() {
                        return new HashSet<>();
                    }

                    @Override
                    public HashSet<String> add(UserBehavior userBehavior, HashSet<String> strings) {
                        strings.add(userBehavior.getUserId());
                        return strings;
                    }

                    @Override
                    public Long getResult(HashSet<String> strings) {
                        return (long) strings.size();
                    }

                    @Override
                    public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, String, Boolean, TimeWindow>() {

                    @Override
                    public void process(Boolean b, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) {
                        String start = new Timestamp(context.window().getStart()).toString();
                        String end = new Timestamp(context.window().getEnd()).toString();
                        Long count = iterable.iterator().next();
                        collector.collect("窗口" + start + "~" + end + "的UV统计值是：" + count);
                    }
                })
                .print();
        executionEnvironment.execute();
    }
}
