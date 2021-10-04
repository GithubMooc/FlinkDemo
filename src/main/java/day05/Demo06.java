package day05;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 02:18
 * @Name 流的合并 union多条流合并，事件类型一样
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Integer> stream1 = executionEnvironment.fromElements(1, 2);
        DataStreamSource<Integer> stream2 = executionEnvironment.fromElements(3, 4);
        DataStreamSource<Integer> stream3 = executionEnvironment.fromElements(5, 6);
        DataStream<Integer> union = stream1.union(stream2, stream3);
        union.print();
        executionEnvironment.execute();
    }
}