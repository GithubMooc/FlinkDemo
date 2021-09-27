package day03;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @Author Master
 * @Date 2021/9/27
 * @Time 22:38
 * @Name ListState 列表状态变量
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random r = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {

                while (running) {
                    int t = r.nextInt(1000);
                    System.out.println(t);
                    sourceContext.collect(t);
                    Thread.sleep(300);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).keyBy(r -> true).process(new KeyedProcessFunction<Boolean, Integer, Double>() {

            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Types.INT)); //Types.INT写成Integer.class也不报错
            }

            @Override
            public void processElement(Integer integer, KeyedProcessFunction<Boolean, Integer, Double>.Context context, Collector<Double> collector) throws Exception {
                listState.add(integer);
                Integer sum = 0;
                Integer count = 0;

                for (Integer i : listState.get()) {
                    sum += i;
                    count += 1;
                }
                collector.collect((double) sum / count);
            }
        }).print();

        executionEnvironment.execute();
    }
}
