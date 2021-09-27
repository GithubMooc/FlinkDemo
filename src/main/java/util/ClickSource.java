package util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import pojo.Event;

import java.util.*;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 17:05
 * @Name FlinkDemo
 */
//    SourceFunction的并行度只能为1
//    ParallelSourceFunction可以自定义并行化的程序
//    生产环境一般使用kafka作为数据源
public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    private String[] userArr = {"Mary", "Bob", "Alice", "Liz"};
    private String[] urlArr = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
    Random random = new Random();

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (running) {
            ctx.collect(new Event(userArr[random.nextInt(userArr.length)], urlArr[random.nextInt(urlArr.length)], Calendar.getInstance().getTimeInMillis()));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}