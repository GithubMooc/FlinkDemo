package day03;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojo.Event;
import util.ClickSource;

/**
 * @Author Master
 * @Date 2021/9/27
 * @Time 22:50
 * @Name 字典状态变量 MapState  计算pv平均值  总浏览 ÷ 总用户
 */
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.addSource(new ClickSource()).keyBy(r -> 1).process(new KeyedProcessFunction<Integer, Event, String>() {
            private MapState<String, Long> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state", Types.STRING, Types.LONG));
            }

            @Override
            public void processElement(Event event, KeyedProcessFunction<Integer, Event, String>.Context context, Collector<String> collector) throws Exception {
                if (mapState.contains(event.getUser())) {
                    mapState.put(event.getUser(), mapState.get(event.getUser()) + 1L);
                } else {
                    mapState.put(event.getUser(), 1L);
                }

                long userNum = 0L;
                long pvSum = 0L;
                for (String key : mapState.keys()) {
                    userNum += 1L;
                    pvSum += mapState.get(key);
                }
                collector.collect("当前pv的平均值是：" + (double) pvSum / userNum);
            }
        }).print();
        executionEnvironment.execute();
    }
}
