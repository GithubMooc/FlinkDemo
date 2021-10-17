package day05;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 15:57
 * @Name connect连接流测试
 */
public class Demo11 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Integer> stream1 = executionEnvironment.fromElements(1, 2);
        DataStreamSource<Integer> stream2 = executionEnvironment.fromElements(3, 4);
        ConnectedStreams<Integer, Integer> connect = stream1.connect(stream2);
        connect.getFirstInput().print("first");
        connect.getSecondInput().print("second");
        executionEnvironment.execute();
    }
}
