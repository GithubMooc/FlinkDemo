package day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.*;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 20:43
 * @Name 实时热门商品 每个商品在每个窗口中的浏览次数
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.readTextFile("input/UserBehavior.csv").map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        return userBehavior.getTimestamp();
                                    }
                                })
                ).keyBy(r -> r.getUserId())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior userBehavior, Long aLong) {
                        return aLong + 1L;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
                        collector.collect(new ItemViewCount(s, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
                    }
                }).print();
        executionEnvironment.execute();
    }
}
