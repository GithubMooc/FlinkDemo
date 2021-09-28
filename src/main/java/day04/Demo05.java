package day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 09:54
 * @Name WaterMark+定时器
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        executionEnvironment
                .socketTextStream("localhost", 9999)
                .map((String s) -> {
                    String[] s1 = s.split(" ");
                    return Tuple2.of(s1[0], Long.parseLong(s1[1]) * 1000L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                })
                ).keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {

                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, KeyedProcessFunction<String, Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect("当前水位线是：" + context.timerService().currentWatermark() + "！");
                        context.timerService().registerEventTimeTimer(stringLongTuple2.f1 + 5000L);
                        collector.collect("注册了一个时间戳是：" + new Timestamp(stringLongTuple2.f1 + 5000L) + "的定时器！");
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("定时器触发了！");
                    }
                })
                .print();


        executionEnvironment.execute();
    }
}
