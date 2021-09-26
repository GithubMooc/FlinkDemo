package day02;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.Event;
import util.*;

/**
 * filter的使用
 */
public class Demo03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        eventDataStreamSource.filter(r->r.user.equals("Mary")).print();

        eventDataStreamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.getUser().equals("Mary");
            }
        }).print();
eventDataStreamSource.filter(new MyFilter()).print();
    }
}
