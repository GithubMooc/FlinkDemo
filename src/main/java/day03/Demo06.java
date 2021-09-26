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
 * @Time 01:24
 * @Name 状态变量的使用，整数连续1秒上升 报警
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.addSource(new SourceFunction<Integer>() {
            private boolean running=true;
            private Random r=new Random();
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
                running=false;
            }
        }).keyBy(r->true).process(new KeyedProcessFunction<Boolean, Integer, String>() {
            private ValueState<Integer> lastInt;
            private ValueState<Long> timerTs;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastInt=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last-integer", Types.INT));
                timerTs=getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
            }

            @Override
            public void processElement(Integer integer, KeyedProcessFunction<Boolean, Integer, String>.Context context, Collector<String> collector) throws Exception {
                Integer prevInt=null;
                if (lastInt.value()!=null) {
                    prevInt=lastInt.value();
                }
                lastInt.update(integer);
                Long ts=null;
                if(timerTs.value()!=null){
                    ts= timerTs.value();
                }
                if(prevInt==null||integer<=prevInt){
                    if(ts!=null){
                        context.timerService().deleteProcessingTimeTimer(ts);
                        timerTs.clear();
                        System.out.println("清除定时器！");
                    }
                }else if(integer>prevInt&&ts==null){
                    long oneSecLater = context.timerService().currentProcessingTime() + 1000L;
                    context.timerService().registerProcessingTimeTimer(oneSecLater);
                    timerTs.update(oneSecLater);
                    System.out.println("注册定时器！");
                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Integer, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                out.collect("温度连续上升了！");
                timerTs.clear();
            }
        }).print();
        executionEnvironment.execute();
    }
}
