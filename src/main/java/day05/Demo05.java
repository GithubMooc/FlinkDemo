package day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Master
 * @Date 2021/9/29
 * @Time 21:02
 * @Name 自定义水位线 WaterMarkGenerate 单流
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] s1 = s.split(" ");
                        return Tuple2.of(s1[0], Long.parseLong(s1[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
                    @Override
                    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Tuple2<String, Long>>() {
                            private Long bound=5000L;
                            private Long maxTs=-Long.MAX_VALUE+bound+1L;
                            @Override
                            public void onEvent(Tuple2<String, Long> stringLongTuple2, long l, WatermarkOutput watermarkOutput) {
                                maxTs=Math.max(maxTs,stringLongTuple2.f1);
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                                watermarkOutput.emitWatermark(new Watermark(maxTs-bound-1L));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new SerializableTimestampAssigner<Tuple2<String,Long>>(){
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                return stringLongTuple2.f1;
                            }
                        };
                    }
                }).print();
        env.execute();
    }
}
