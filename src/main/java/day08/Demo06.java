package day08;

import day06.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.*;

import java.util.*;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 00:45
 * @Name CEP处理超时事件
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);


        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = executionEnvironment.fromElements(
                        new Event("order1", "create", 1000L),
                        new Event("order2", "create", 2000L),
                        new Event("order1", "pay", 3000L)
                        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

        Pattern<Event, Event> pattern = Pattern.<Event>begin("create")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getEventType().equals("create");
                    }
                }).next("pay").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getEventType().equals("pay");
                    }
                }).within(Time.seconds(5));
        PatternStream<Event> patternStream = CEP.pattern(eventSingleOutputStreamOperator.keyBy(Event::getOrderId), pattern);

        SingleOutputStreamOperator<String> result = patternStream.flatSelect(new OutputTag<String>("timeout"){}, new PatternFlatTimeoutFunction<Event, String>() {
            @Override
            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                Event create = map.get("create").get(0);
                collector.collect("订单：" + create.getOrderId() + "超时了！");
            }
        }, new PatternFlatSelectFunction<Event, String>() {
            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                Event pay = map.get("pay").get(0);
                collector.collect("订单：" + pay.getOrderId() + "已支付！");
            }
        });
        result.print("main>>>>>>>>>>");
        result.getSideOutput(new OutputTag<String>("timeout"){}).print("side>>>>>>>>>>");
        executionEnvironment.execute();
    }
}
