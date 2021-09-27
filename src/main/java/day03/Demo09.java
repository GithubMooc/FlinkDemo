package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.Event;
import util.ClickSource;

import java.sql.Timestamp;

/**
 * @Author Master
 * @Date 2021/9/27
 * @Time 23:31
 * @Name 全窗口聚合函数 Flink Window API
 */
public class Demo09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .addSource(new ClickSource())
                .keyBy(r -> r.getUser())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowResult())
                .print();
        executionEnvironment.execute();
    }


    private static class WindowResult extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, java.lang.Iterable<Event> iterable, Collector<String> collector) throws Exception {
//            在窗口关闭的时候，触发调用
//            迭代器参数中包含了窗口中左右的元素
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            long count = iterable.spliterator().getExactSizeIfKnown(); //迭代器里面共多少条元素
            collector.collect("用户：" + s + "在窗口" + new Timestamp(windowStart) + "-" + new Timestamp(windowEnd) + "中的pv次数是：" + count);
        }
    }
}
