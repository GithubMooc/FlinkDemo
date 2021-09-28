package day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 09:20
 * @Name 设置插入水位线的间隔时间
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.getConfig().setAutoWatermarkInterval(60 * 1000L);
        executionEnvironment
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] s1 = s.split(" ");
                        return Tuple2.of(s1[0], Long.parseLong(s1[1]) * 1000L);
                    }
                })
                //默认每隔200毫秒的机器时间，插入一次水位线
                .assignTimestampsAndWatermarks(
//                        最大延迟时间设置为0秒
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long l) {
                                        return element.f1; //告诉Flink事件时间是哪一个字段
                                    }
                                }))
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
//                5s的事件时间滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) {
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        long count = iterable.spliterator().getExactSizeIfKnown();
                        collector.collect("用户：" + s + "在窗口" + new Timestamp(windowStart) + "-" + new Timestamp(windowEnd) + "中的pv次数是：" + count);
                    }
                })
                .print();
        executionEnvironment.execute();
    }
}
