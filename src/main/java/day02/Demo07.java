package day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 18:05
 * @Name 重新分区
 */
public class Demo07 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromElements(1,2,3,4,5,6,7).setParallelism(1).shuffle().print("shuffer").setParallelism(2);

        executionEnvironment.fromElements(1,2,3,4,5,6,7).setParallelism(1).rebalance().print("rebalance").setParallelism(2);

        executionEnvironment.fromElements(1,2,3,4,5,6,7).setParallelism(1).broadcast().print("broadcast").setParallelism(2);

        executionEnvironment.execute();
    }
}
