package day07;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import pojo.UserBehavior;

import java.sql.Timestamp;
import java.util.*;

/**
 * @Author Master
 * @Date 2021/10/6
 * @Time 23:55
 * @Name FlinkDemo
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        executionEnvironment.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties))
                .map((MapFunction<String, UserBehavior>) s -> {
                    String[] split = s.split(",");
                    return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                }).filter(r-> "pv".equals(r.getBehavior())).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (userBehavior, l) -> userBehavior.getTimestamp())).keyBy(r -> true)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .process(new ProcessWindowFunction<UserBehavior, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, ProcessWindowFunction<UserBehavior, String, Boolean, TimeWindow>.Context context, Iterable<UserBehavior> iterable, Collector<String> collector) {
                        Map<String, Long> map = new HashMap<>();
                        for (UserBehavior userBehavior : iterable) {
                            if (map.containsKey(userBehavior.getItemId())) {
                                map.put(userBehavior.getUserId(), map.get(userBehavior.getItemId() + 1L));
                            } else {
                                map.put(userBehavior.getItemId(), 1L);
                            }
                        }
                        List<Tuple2<String, Long>> list = new ArrayList<>();
                        for (String s : map.keySet()) {
                            list.add(Tuple2.of(s, map.get(s)));
                        }
                        list.sort((o1, o2) -> o2.f1.intValue() - o1.f1.intValue());
                        StringBuilder result = new StringBuilder();
                        result.append("=====================\n").append("窗口：").append(new Timestamp(context.window().getStart())).append("--").append(new Timestamp(context.window().getEnd())).append("\n");
                        for (int i = 0; i < 3; i++) {
                            Tuple2<String, Long> temp = list.get(i);
                            result.append("第").append(i + 1).append("名的商品ID是：").append(temp.f0).append("，浏览次数是：").append(temp.f1).append("\n");
                        }
                        collector.collect(new String(result));
                    }
                }).print();
        executionEnvironment.execute();
    }
}
