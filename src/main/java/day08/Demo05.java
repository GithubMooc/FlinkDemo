package day08;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 00:20
 * @Name 有限状态机，使用状态机检测连续三次登录失败
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);


        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = executionEnvironment.fromElements(new Event("user1", "fail", 1000L), new Event("user1", "fail", 2000L), new Event("user1", "fail", 3000L), new Event("user2", "success", 3000L), new Event("user1", "fail", 4000L)).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));
        eventSingleOutputStreamOperator.keyBy(r -> r.getUser())
                .process(new KeyedProcessFunction<String, Event, String>() {
                    //                            状态机
                    private HashMap<Tuple2<String, String>, String> stateMachine = new HashMap<>();
                    //                            当前状态
                    private ValueState<String> currentState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
//                                状态转移矩阵
//                                key：（状态，接收到事件的类型）
//                                value：将要跳转的状态
                        stateMachine.put(Tuple2.of("INITIAL", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("INITIAL", "fail"), "S1");
                        stateMachine.put(Tuple2.of("S1", "fail"), "S2");
                        stateMachine.put(Tuple2.of("S2", "fail"), "FAIL");
                        stateMachine.put(Tuple2.of("S1", "success"), "SUCCESS");
                        stateMachine.put(Tuple2.of("S2", "success"), "SUCCESS");

                        currentState = getRuntimeContext().getState(new ValueStateDescriptor<String>("current", Types.STRING));
                    }

                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        if (currentState.value() == null) {
                            currentState.update("INITIAL");
                        }

                        String nextState = stateMachine.get(Tuple2.of(currentState.value(), event.getEventType()));
                        if ("FAIL".equals(nextState)) {
                            collector.collect("用户：" + event.getUser() + "连续三次登录失败了！");
                            currentState.update("S2");
                        } else if ("SUCCESS".equals(nextState)) {
                            currentState.clear();
                        } else {
                            currentState.update(nextState);
                        }
                    }
                }).print();
        executionEnvironment.execute();
    }
}
