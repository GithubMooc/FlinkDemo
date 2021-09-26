package day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 23:26
 * @Name 富函数 RichMapFunction
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3);

        integerDataStreamSource.map(new RichMapFunction<Integer, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("生命周期方法开始");
                System.out.println("当前子任务的索引是：" + getRuntimeContext().getIndexOfThisSubtask());
                super.open(parameters);
            }

            @Override
            public Integer map(Integer integer) {
                return integer * integer;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("生命周期方法结束");
            }
        }).print();
        executionEnvironment.execute();
    }
}
