package day01;

//        从socket读取数据后处理
//        Word Count

import lombok.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Master
 * 从socket读取数据后处理
 * Word Count
 */
public class Demo01 {
    //    抛出异常
    public static void main(String[] args) throws Exception {
//        TODO 1.获取流处理的运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        TODO 2.设置并行任务的数量为1
        env.setParallelism(1);
//        TODO 3.读取数据源
//        先在终端启动`nc -lp 9999`(windows) `nc -lk 9999`(Linux) l表示监听，p/k表示 保持当前连接
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);
//        TODO 4.map操作:针对流中的每一个元素，输出一个元素
//        这里使用的是flatMap：针对流中的每一个元素，输出0个，1个或者多个元素
//        输入泛型：String，输出泛型：WordWithCount

        SingleOutputStreamOperator<WordWithCount> mappedStream = stream.flatMap(new FlatMapFunction<String, WordWithCount>() {

            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String[] arr = value.split(" ");
                for (String s : arr) {
                    out.collect(new WordWithCount(s, 1L));
                }
            }
        });
//        TODO 4.分组shuffer
//        第一个泛型：流中元素的泛型
//        第二个泛型：key的泛型
        KeyedStream<WordWithCount, String> keyedStream = mappedStream.keyBy(new KeySelector<WordWithCount, String>() {
            @Override
            public String getKey(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word;
            }
        });
//        TODO 5.reduce操作
//        reduce会维护一个累加器
//        第一条数据到来，作为累加器输出
//        第二条数据到来，和累加器进行聚合操作，然后输出累加器
//        累加器和流中元素的类型是一样的
        SingleOutputStreamOperator<WordWithCount> result = keyedStream.reduce(new ReduceFunction<WordWithCount>() {
            @Override
            public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
                return new WordWithCount(w1.word, w1.count + w2.count);
            }
        });
        result.print();
        env.execute();
    }
//    POJO类 区别于java中的Bean
//    必须是公共类
//    所有字段必须是public
//    必须有空构造器
//    模拟了case class

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode
    public static class WordWithCount {
        public String word;
        public Long count;
    }
}
