package day05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.*;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 01:27
 * @Name 迟到数据发送到侧输出流 无窗口
 */
public class Demo02 {private static OutputTag<String> outputTag=new OutputTag<String>("late"){};
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<String> result = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
//                指定时间发送数据
                sourceContext.collectWithTimestamp(Tuple2.of("Hello World", 1000L), 1000L);
//                发送水位线
                sourceContext.emitWatermark(new Watermark(999L));

                sourceContext.collectWithTimestamp(Tuple2.of("Hello Flink", 2000L), 2000L);
                sourceContext.emitWatermark(new Watermark(1999L));
                sourceContext.collectWithTimestamp(Tuple2.of("Hello Late", 1000L), 1000L);
            }

            @Override
            public void cancel() {

            }
        }).process(new ProcessFunction<Tuple2<String, Long>, String>() {
            @Override
            public void processElement(Tuple2<String, Long> stringLongTuple2, ProcessFunction<Tuple2<String, Long>, String>.Context context, Collector<String> collector) throws Exception {
                if (stringLongTuple2.f1 < context.timerService().currentWatermark()) {
                    context.output(outputTag, "迟到的元素发送到侧输出流：" + stringLongTuple2);

                } else {
                    collector.collect("正常到达的元素：" + stringLongTuple2);
                }
            }
        });

        result.print("主流数据：");
        result.getSideOutput(outputTag).print("侧输出流：");

        env.execute();
    }
}
