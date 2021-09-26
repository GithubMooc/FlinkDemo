package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Author Master
 * @Date 2021/9/27
 * @Time 00:07
 * @Name KeyedProcessFunction 定时器
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.socketTextStream("localhost",9999).keyBy(r->1).process(new MyKeyed()).print();
        executionEnvironment.execute();
    }
    public static class MyKeyed extends KeyedProcessFunction<Integer, String,String>{

        @Override
        public void processElement(String s, KeyedProcessFunction<Integer, String, String>.Context context, Collector<String> collector) throws Exception {
            long ts = context.timerService().currentProcessingTime();
            collector.collect("元素：" + s + "在" + new Timestamp(ts) + "到达！");
//            注册一个10秒之后的定时器
            long tenSecLater = ts + 10 * 1000L;
            collector.collect("注册了一个时间在："+new Timestamp(tenSecLater)+"的定时器？");
//            定时器注册的语法
            context.timerService().registerProcessingTimeTimer(tenSecLater);
        }

//        定义定时器
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Integer, String, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！触发时间是："+new Timestamp(timestamp));
        }
    }
}
