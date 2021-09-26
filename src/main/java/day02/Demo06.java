package day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 17:35
 * @Name 实时计算平均数
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        executionEnvironment.addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random r = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(r.nextInt(10));
                            Thread.sleep(100);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                }).map(r -> Tuple2.of(r, 1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy(r -> true)
                .reduce((Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) -> Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1))
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .map(r -> (double) r.f0 / r.f1)
                .print();
        executionEnvironment.execute();
    }
}
