package day06;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.ClickSource;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 23:56
 * @Name 状态 保存点
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStateBackend(new FsStateBackend("file:///D:/checkpoint"));
//        10秒一次
        executionEnvironment.enableCheckpointing(10 * 1000L);
        executionEnvironment.addSource(new ClickSource()).print();
        executionEnvironment.execute();
    }
}
