package day06;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author Master
 * @Date 2021/10/4
 * @Time 23:57
 * @Name 基于窗口的join
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream1 = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2))                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().withTimestampAssigner((stringIntegerTuple2, l) -> stringIntegerTuple2.f1));

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = executionEnvironment.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("b", 3))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps().withTimestampAssigner((stringIntegerTuple2, l) -> stringIntegerTuple2.f1));
   stream1.join(stream2).where(r->r.f0).equalTo(r->r.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
       @Override
       public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
           return first+"=>"+second;
       }
   }).print();
   executionEnvironment.execute();
    }
}
