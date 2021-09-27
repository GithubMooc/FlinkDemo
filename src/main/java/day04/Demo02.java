package day04;

import org.apache.flink.api.common.functions.AggregateFunction;
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
 * @Date 2021/9/28
 * @Time 00:45
 * @Name 增量聚合函数与全窗口聚合函数结合使用
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .addSource(new ClickSource())
                .keyBy(r -> r.getUser())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
                .aggregate(
                new AggregateFunction<Event, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Event event, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Integer, String, String, TimeWindow>() {

                    //迭代器参数iterable中只包含一个元素，就是增量聚合函数发送过来的聚合结果
                    @Override
                    public void process(String s, ProcessWindowFunction<Integer, String, String, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = iterable.iterator().next();
                        collector.collect("用户：" + s + "在窗口" + new Timestamp(start) + "-" + new Timestamp(end) + "中的pv次数是：" + count);
                    }
                }
        ).print();
        executionEnvironment.execute();
    }
}
