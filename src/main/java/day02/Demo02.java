package day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * map的使用
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(random.nextInt(1000));
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
//                running = false;
            }
        });
        // TODO 使用匿名内部类
        integerDataStreamSource.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                        return Tuple2.of(integer, integer);
                    }
                })
                .print();
        //自定义类
        integerDataStreamSource.map(new MyMap()).print();

//        TODO 使用lambda
        integerDataStreamSource.map(r -> Tuple2.of(r, r))
//                TODO 需要returns方法来标注map的返回值类型
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        env.execute();
    }

    public static class MyMap implements MapFunction<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
            return Tuple2.of(integer, integer);
        }
    }
}
