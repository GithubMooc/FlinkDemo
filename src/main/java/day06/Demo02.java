package day06;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2021/9/30
 * @Time 08:52
 * @Name 实时对账
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Event> orderStream = executionEnvironment.fromElements(new Event("order1", "order", 1000L), new Event("order2", "order", 2000L)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((event, l) -> event.getTimestamp())
        );
        SingleOutputStreamOperator<Event> weixinStream = executionEnvironment.fromElements(new Event("order1", "weixin", 3000L), new Event("order3", "weixin", 4000L)).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner((event, l) -> event.getTimestamp())
        );

        orderStream
                .keyBy(r -> r.getOrderId())
                .connect(weixinStream.keyBy(r -> r.getOrderId()))
                .process(new CoProcessFunction<Event, Event, String>() {
                    private ValueState<Event> orderState;
                    private ValueState<Event> weixinState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("order", Types.POJO(Event.class)));
                        weixinState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("weixin", Types.POJO(Event.class)));
                    }

                    @Override
                    public void processElement1(Event event, Context context, Collector<String> collector) throws Exception {
                        if (weixinState.value() == null) {
                            //下订单order事件先到达，因为如果微信事件先到达，那么weixinState就不为空了
                            orderState.update(event);
                            context.timerService().registerEventTimeTimer(event.getTimestamp() + 5000L);
                        } else {
                            collector.collect("订单ID是：" + event.getOrderId() + "，对账成功，weixin事件先到达！");
                            weixinState.clear();
                        }

                    }

                    @Override
                    public void processElement2(Event event, Context context, Collector<String> collector) throws Exception {
                        if (orderState.value() == null) {
                            weixinState.update(event);
                            context.timerService().registerEventTimeTimer(event.getTimestamp() + 5000L);
                        } else {
                            collector.collect("订单ID是：" + event.getOrderId() + "，对账成功，order事件先到达！");
                            orderState.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (orderState.value() != null) {
                            out.collect("订单ID是：" + orderState.value().getOrderId() + "，对账失败，weixin事件5秒内未到达！");
                            orderState.clear();
                        }
                        if (weixinState.value() != null) {
                            out.collect("订单ID是：" + weixinState.value().getOrderId() + "，对账失败，order事件5秒内未到达！");
                            weixinState.clear();
                        }
                    }
                }).print();
        executionEnvironment.execute();
    }
}
