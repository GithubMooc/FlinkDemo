package day06;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Master
 * @Date 2021/9/30
 * @Time 00:18
 * @Name FlinkDemo
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream1 = executionEnvironment.fromElements(Tuple2.of("a", 1),Tuple2.of("b", 2));
        DataStreamSource<Tuple2<String, String>> stream2 = executionEnvironment.fromElements(Tuple2.of("a", "a"), Tuple2.of("b", "b"));
        stream1
                .keyBy(r -> r.f0)
                .connect(stream2)
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {
                    private ListState<Tuple2<String, Integer>> list1;
                    private ListState<Tuple2<String, String>> list2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        list1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("list1", Types.TUPLE(Types.STRING, Types.INT)));
                        list2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, String>>("list2", Types.TUPLE(Types.STRING, Types.STRING)));
                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> stringIntegerTuple2, CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>.Context context, Collector<String> collector) throws Exception {
                        list1.add(stringIntegerTuple2);
                        for (Tuple2<String, String> e : list2.get()) {
                            collector.collect(stringIntegerTuple2 + "=>" + e);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> stringStringTuple2, CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>.Context context, Collector<String> collector) throws Exception {
                        list2.add(stringStringTuple2);
                        for (Tuple2<String, Integer> e : list1.get()) {
                            collector.collect(stringStringTuple2 + "=>" + e);
                        }
                    }
                })
                .print();
        executionEnvironment.execute();
    }
}
