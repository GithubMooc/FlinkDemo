package day04;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import pojo.Event;
import util.ClickSource;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 00:28
 * @Name 增量聚合函数：AggregateFunction累加 每隔5秒钟窗口的pv 使用增量聚合函数
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment
                .addSource(new ClickSource())
                .keyBy(r->r.getUser())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
                .aggregate(new CountAgg())
                .print();
        executionEnvironment.execute();
    }

//    输入泛型，累加器泛型，输出泛型
    private static class CountAgg implements AggregateFunction<Event,Integer,Integer> {
//        创建累加器
        @Override
        public Integer createAccumulator() {
            return 0;
        }
//        定义累加规则
        @Override
        public Integer add(Event event, Integer integer) {
            return integer+1;
        }
//        在窗口关闭是返回结果
        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }
}
