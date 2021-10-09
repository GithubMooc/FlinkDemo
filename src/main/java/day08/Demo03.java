package day08;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/10/8
 * @Time 23:43
 * @Name 使用FlinkCEP检测连续3次登录失败
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = executionEnvironment.fromElements(new Event("user1", "fail", 1000L), new Event("user1", "fail", 2000L), new Event("user2", "success", 3000L), new Event("user1", "fail", 4000L)).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));

        Pattern<Event, Event> pattern = Pattern.<Event>begin("first").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEventType().equals("fail");
            }
        }).next("second").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEventType().equals("fail");
            }
        }).next("third").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getEventType().equals("fail");
            }
        });

        PatternStream<Event> patternStream = CEP.pattern(eventSingleOutputStreamOperator.keyBy(r -> r.getUser()), pattern);

        patternStream.select(map -> {
//                map的key是给事件取得名字
//                列表是名字对应的事件所构成的列表
            Event first = map.get("first").get(0);
            Event second = map.get("second").get(0);
            Event third = map.get("third").get(0);

            String s = "用户：<" + first.getUser() + ">在时间：（" + first.getTimestamp() + "，" + second.getTimestamp() + "，" + third.getTimestamp() + "）登录失败了！";
            return s;
        }).print();
        executionEnvironment.execute();
    }
}
