package day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 01:13
 * @Name 侧输出流
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.socketTextStream("localhost", 9999).map(r -> {
                    String[] s = r.split(" ");
                    return Tuple2.of(s[0], Long.parseLong(s[1]) * 1000L);
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        //WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps().withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                }
                        )
                ).process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, ProcessFunction<Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                        if (stringLongTuple2.f1 < context.timerService().currentWatermark()) {
                            collector.collect("元素迟到了：" + stringLongTuple2);
                        } else {
                            collector.collect(stringLongTuple2 + "元素没有迟到");
                        }
//                        context.timerService().registerEventTimeTimer(context.timerService().currentWatermark()+1000L);
                    }

                    //不能使用定时器，运行时报错
//                    @Override
//                    public void onTimer(long timestamp, ProcessFunction<Tuple2<String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//                        super.onTimer(timestamp, ctx, out);
//                    }
                }).print();
        executionEnvironment.execute();
    }
}
