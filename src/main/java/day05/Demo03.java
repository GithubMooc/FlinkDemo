package day05;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.*;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 01:58
 * @Name 迟到数据输出到侧输出流 有窗口
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        SingleOutputStreamOperator<String> process = executionEnvironment
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> sourceContext) throws Exception {
                        sourceContext.collectWithTimestamp("a", 1000L);
                        sourceContext.emitWatermark(new Watermark(999L));
                        sourceContext.collectWithTimestamp("a", 2000L);
                        sourceContext.emitWatermark(new Watermark(1999L));
                        sourceContext.collectWithTimestamp("a", 4000L);
                        sourceContext.emitWatermark(new Watermark(4999L));
                        sourceContext.collectWithTimestamp("a", 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                }).keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<String>("late") {
                })
                .process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        collector.collect("窗口中共有：" + iterable.spliterator().getExactSizeIfKnown()+"条数据");
                    }
                });
        process.print();
        process.getSideOutput(new OutputTag<String>("late") {
        }).print("late");
        executionEnvironment.execute();
    }
}
