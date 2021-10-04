package day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.*;

import java.time.Duration;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 20:20
 * @Name 使用迟到数据更新窗口计算结果
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> process = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] s1 = s.split(" ");
                        return Tuple2.of(s1[0], Long.parseLong(s1[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }))
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late"){})
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
//                        初始化一个窗口状态变量，注意：窗口状态变量的可见范围是当前窗口
                        ValueState<Boolean> firstCalculate = context.windowState().getState(new ValueStateDescriptor<Boolean>("first", Types.BOOLEAN));
                        if (firstCalculate.value() == null) {
                            collector.collect("窗口第一次触发计算了，水位线是：" + context.currentWatermark() + "，窗口中共有：" + iterable.spliterator().estimateSize() + "条元素！");
//                            第一次触发process执行以后，更新为true
                            firstCalculate.update(true);
                        } else {
                            collector.collect("迟到数据到了，更新以后的计算结果是：" + iterable.spliterator().getExactSizeIfKnown());
                        }
                    }
                });
        process.print("主流>>>>>>>>>>>>>>>>>>>>>>");
        process.getSideOutput(new OutputTag<Tuple2<String, Long>>("late"){}).print("侧输出流>>>>>>>>>>>>>>>>>>");
        env.execute();
    }
}
