package day03;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Author Master
 * @Date 2021/9/27
 * @Time 00:33
 * @Name 状态变量：ValueState
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random r = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running) {
                    sourceContext.collect(r.nextInt(10));
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).keyBy(r -> true).process(new KeyedProcessFunction<Boolean, Integer, Double>() {
            //            声明一个状态变量作为累加器
//            状态变量可见范围是当前key
//            状态变量是单例，只能被实例化一次
            private ValueState<Tuple2<Integer, Integer>> valueState;
            private ValueState<Long> timerTs;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //实力化状态变量
                valueState = getRuntimeContext().getState(
//                        状态描述符
                        new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-count", Types.TUPLE(Types.INT, Types.INT)));
                timerTs = getRuntimeContext().getState(
                        new ValueStateDescriptor<Long>("timer", Types.LONG));
            }

            @Override
            public void processElement(Integer integer, KeyedProcessFunction<Boolean, Integer, Double>.Context context, Collector<Double> collector) throws Exception {
//                当第一条数据到来时，状态变量的值为null
//                使用.value()方法获取状态变量的值，使用.update()方法更新状态变量的值
                if (valueState.value() == null) {
                    valueState.update(Tuple2.of(integer, 1));
                } else {
                    Tuple2<Integer, Integer> tmp = valueState.value();
                    valueState.update(Tuple2.of(tmp.f0 + integer, tmp.f1 + 1));
                }
//                collector.collect((double) valueState.value().f0 / valueState.value().f1);

                if (timerTs.value() == null) {
                    long tenSecLater = context.timerService().currentProcessingTime() + 10000L;
                    context.timerService().registerProcessingTimeTimer(tenSecLater);
                    timerTs.update(tenSecLater);
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Integer, Double>.OnTimerContext ctx, Collector<Double> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                if (valueState != null) {
                    out.collect((double) valueState.value().f0 / valueState.value().f1);
                    timerTs.clear();
                }
            }
        }).print();

        executionEnvironment.execute();
    }
}
