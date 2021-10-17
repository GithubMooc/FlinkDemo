package day07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author Master
 * @Date 2021/10/5
 * @Time 15:49
 * @Name 写入kafka
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        executionEnvironment.readTextFile("input/UserBehavior.csv").addSink(new FlinkKafkaProducer<>("test", new SimpleStringSchema(), properties));
        executionEnvironment.execute();
    }
}
