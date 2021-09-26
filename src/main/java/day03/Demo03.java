package day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 23:52
 * @Name 自定义输出 addSink
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.fromElements(1, 2, 3)
                .addSink(new RichSinkFunction<Integer>() {
            @Override
            public void invoke(Integer value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println(value);
            }
        });
        executionEnvironment.execute();
    }
}
