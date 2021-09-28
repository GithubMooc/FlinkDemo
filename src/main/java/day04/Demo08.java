package day04;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojo.*;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 20:43
 * @Name 实时热门商品 每个窗口浏览次数前三名
 */
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]) * 1000L);
                    }
                })
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior userBehavior) throws Exception {
                        return userBehavior.getBehavior().equals("pv");
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        return userBehavior.getTimestamp();
                                    }
                                })
                ).keyBy(r -> r.getUserId())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<UserBehavior, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(UserBehavior userBehavior, Long aLong) {
                        return aLong + 1L;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
                        collector.collect(new ItemViewCount(s, iterable.iterator().next(), context.window().getStart(), context.window().getEnd()));
                    }
                })
                .keyBy(r -> r.getWindowEnd())
                .process(new KeyedProcessFunction<Long, ItemViewCount, String>() {

                    private ListState<ItemViewCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("list", Types.POJO(ItemViewCount.class)));
                    }

                    @Override
                    public void processElement(ItemViewCount itemViewCount, KeyedProcessFunction<Long, ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {
                        listState.add(itemViewCount);
                        context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        List<ItemViewCount> list = new ArrayList<>();
                        for (ItemViewCount itemViewCount : listState.get()) {
                            list.add(itemViewCount);
                        }
                        list.sort(new Comparator<ItemViewCount>() {
                            @Override
                            public int compare(ItemViewCount o1, ItemViewCount o2) {
                                return o2.getCount().intValue() - o1.getCount().intValue();
                            }
                        });
                        StringBuilder result = new StringBuilder();
                        result.append("================================\n").append("窗口结束时间：" + new Timestamp(timestamp - 1L)).append("\n");
                        for (int i = 0; i < 3; i++) {
                            ItemViewCount itemViewCount = list.get(i);
                            result.append("第" + (i + 1) + "名的商品ID是：" + itemViewCount.getItemId() + ",浏览次数是：" + itemViewCount.getCount() + "次！\n");
                        }
                        result.append("================================\n\n\n");
                        out.collect(result.toString());
                    }
                })
                .print();
        executionEnvironment.execute();
    }
}
