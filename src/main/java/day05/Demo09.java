package day05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import pojo.Event;
import util.ClickSource;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 23:37
 * @Name connect链接两条流
 * 只能链接两条流
 * 流中元素可以使不同的类型
 * 查询流
 * 当在socketStream 输入./home时，clickStream流中的./home被放行
 */
public class Demo09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clickStream = env.addSource(new ClickSource());
        DataStreamSource<String> socketStream = env.socketTextStream("localhost", 9999).setParallelism(1);
        clickStream.keyBy(r -> r.getUser()).connect(socketStream.broadcast()).flatMap(new CoFlatMapFunction<Event, String, Event>() {
            private String query = "";

            @Override
            public void flatMap1(Event event, Collector<Event> collector) throws Exception {
                if (event.getUrl().equals(query)) {
                    collector.collect(event);
                }
            }

            @Override
            public void flatMap2(String s, Collector<Event> collector) throws Exception {
                query = s;
            }
        }).print();
        env.execute();
    }
}