package day02;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 17:28
 * @Name 计算数字之和，分别用sum以及reduce
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Tuple2<Integer, Integer>> tuple2DataStreamSource = executionEnvironment.fromElements(
                Tuple2.of(1, 2), Tuple2.of(1, 3)
        );
        KeyedStream<Tuple2<Integer, Integer>, Integer> tuple2IntegerKeyedStream = tuple2DataStreamSource.keyBy(r -> r.f0);

//        tuple2IntegerKeyedStream.sum(1).print();

//        tuple2IntegerKeyedStream.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
//            @Override
//            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> t1) throws Exception {
//                return Tuple2.of(integerIntegerTuple2.f0, integerIntegerTuple2.f1 + t1.f1);
//            }
//        }).print();

        //lambda

        tuple2IntegerKeyedStream.reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1)).print();

        executionEnvironment.execute();
    }
}
