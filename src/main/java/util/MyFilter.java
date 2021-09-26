package util;

import org.apache.flink.api.common.functions.FilterFunction;
import pojo.Event;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 17:10
 * @Name FlinkDemo
 */
public class MyFilter implements FilterFunction<Event>{

    @Override
    public boolean filter(Event event) throws Exception {
        return event.getUser().equals("Mary");
    }
}
