package day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatMap的使用
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("white", "black", "gray");

//        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                if ("white".equals(s)) {
//                    collector.collect(s);
//                }else if("white".equals(s)){
//                    collector.collect(s);
//                    collector.collect(s);
//                }
//            }
//        }).print();
//
        stringDataStreamSource.flatMap((String s, Collector<String> out) -> {
            if ("white".equals(s)) {
                out.collect(s);
            } else if ("black".equals(s)) {
                out.collect(s);
                out.collect(s);
            }
        }).returns(Types.STRING).print();
        env.execute();
    }
}
