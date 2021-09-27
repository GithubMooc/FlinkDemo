package day04;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Event;
import util.ClickSource;

import java.sql.Timestamp;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 00:45
 * @Name 使用key模拟一个5秒的滚动窗口，模拟的是增量聚合函数和券窗口函数聚合使用的情况
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.addSource(new ClickSource()).keyBy(r -> r.getUser()).process(new KeyedProcessFunction<String, Event, String>() {
            //            key是串口的开窗时间，value是窗口中的pv数值(累加器)
            private MapState<Long, Integer> mapState;
            private Long windowSize = 5000L;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Integer>("pv", Types.LONG, Types.INT));
            }

            @Override
            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
//                计算当前元素所属的窗口的开始时间
                long currTime = context.timerService().currentProcessingTime();
                long windowStart = currTime - currTime % windowSize;
                long windowEnd = windowStart + windowSize;

                if (mapState.contains(windowStart)) {
                    mapState.put(windowStart, mapState.get(windowStart) + 1);
                }
                else {mapState.put(windowStart,1);}
                context.timerService().registerProcessingTimeTimer(windowEnd-1L);
            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                long windowEnd = timestamp + 1L;
                long windowStart = windowEnd - windowSize;
                long count = mapState.get(windowStart);
                out.collect("用户：" + ctx.getCurrentKey() + "在窗口" + new Timestamp(windowStart) + "-" + new Timestamp(windowEnd) + "中的pv次数是：" + count);
                mapState.remove(windowStart);
            }
        }).print();
        executionEnvironment.execute();
    }
}
