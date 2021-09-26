package day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 23:42
 * @Name 富函数：RichParallelSourceFunction
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("生命周期方法开始，子任务索引是：" + getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 10; i++) {
                    if(i%2==getRuntimeContext().getIndexOfThisSubtask()){
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).print().setParallelism(2);

        executionEnvironment.execute();
    }
}
