package day06;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2021/9/30
 * @Time 08:52
 * @Name 实时对账 模拟真实环境
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<Event> orderStream = executionEnvironment.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> sourceContext) throws Exception {
                sourceContext.collectWithTimestamp(new Event("order1","order",1000L),1000L);
                sourceContext.emitWatermark(new Watermark(999L));
                sourceContext.collectWithTimestamp(new Event("order2","order",3000L),3000L);
                sourceContext.emitWatermark(new Watermark(8001L));
            }

            @Override
            public void cancel() {

            }
        });
        SingleOutputStreamOperator<Event> weixinStream = executionEnvironment.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> sourceContext) throws Exception {
                sourceContext.collectWithTimestamp(new Event("order1","weixin",4000L),4000L);
                sourceContext.emitWatermark(new Watermark(3999L));
                sourceContext.emitWatermark(new Watermark(8001L));
                sourceContext.collectWithTimestamp(new Event("order2","weixin",9000L),9000L);
            }

            @Override
            public void cancel() {

            }
        });

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
