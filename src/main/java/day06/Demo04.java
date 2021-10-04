package day06;


import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2021/10/4
 * @Time 22:59
 * @Name 基于间隔的join
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Demo04Event> orderStream = executionEnvironment
                .fromElements(new Demo04Event("user1", "order", 20 * 60 * 1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Demo04Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (event, l) -> event.getTimestamp())
                );

        SingleOutputStreamOperator<Demo04Event> pvStream = executionEnvironment
                .fromElements(
                        new Demo04Event("user1", "pv", 5 * 60 * 1000L),
                        new Demo04Event("user1", "order", 10 * 60 * 1000L),
                        new Demo04Event("user1", "order", 12 * 60 * 1000L),
                        new Demo04Event("user1", "order", 22 * 60 * 1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Demo04Event>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (event, l) -> event.getTimestamp())
                );

        orderStream
                .keyBy(r -> r.getUserId())
                .intervalJoin(pvStream
                        .keyBy(r -> r.getUserId()))
                .between(Time.minutes(-10), Time.minutes(5))
                .process(new ProcessJoinFunction<Demo04Event, Demo04Event, String>() {
                    @Override
                    public void processElement(Demo04Event left, Demo04Event right, ProcessJoinFunction<Demo04Event, Demo04Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + "=>" + right);
                    }
                })
                .print("orderStream JOIN pvStream：");
        pvStream.keyBy(r->r.getUserId()).intervalJoin(orderStream.keyBy(r->r.getUserId())).between(Time.minutes(-5), Time.minutes(10)).process(new ProcessJoinFunction<Demo04Event, Demo04Event, String>() {
            @Override
            public void processElement(Demo04Event left, Demo04Event right, ProcessJoinFunction<Demo04Event, Demo04Event, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + "=>" + right);
                    }
                })
                .print("pvStream JOIN orderStream：");
        executionEnvironment.execute();
    }
}
