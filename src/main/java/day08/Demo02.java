package day08;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.UserBehavior;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author Master
 * @Date 2021/10/8
 * @Time 21:23
 * @Name 布隆过滤器
 */
public class Demo02 {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (userBehavior, l) -> userBehavior.getTimestamp())).keyBy(r -> true).window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new AggregateFunction<UserBehavior, Tuple2<Long, BloomFilter<String>>, Long>() {
                    @Override
                    public Tuple2<Long, BloomFilter<String>> createAccumulator() {
                        return Tuple2.of(0L, BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 100000, 0.01));
                    }

                    @Override
                    public Tuple2<Long, BloomFilter<String>> add(UserBehavior userBehavior, Tuple2<Long, BloomFilter<String>> longBloomFilterTuple2) {
                        if (!longBloomFilterTuple2.f1.mightContain(userBehavior.getUserId())) {
                            longBloomFilterTuple2.f1.put(userBehavior.getUserId());
                            longBloomFilterTuple2.f0 += 1L;
                        }
                        return longBloomFilterTuple2;
                    }

                    @Override
                    public Long getResult(Tuple2<Long, BloomFilter<String>> longBloomFilterTuple2) {
                        return longBloomFilterTuple2.f0;
                    }

                    @Override
                    public Tuple2<Long, BloomFilter<String>> merge(Tuple2<Long, BloomFilter<String>> longBloomFilterTuple2, Tuple2<Long, BloomFilter<String>> acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, String, Boolean, TimeWindow>() {

                    @Override
                    public void process(Boolean b, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
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