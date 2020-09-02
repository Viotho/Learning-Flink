package FlinkStreaming.KafkaConnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop-01");
        properties.setProperty("group.id", "con1");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromGroupOffsets();

        DataStream<String> text = env.addSource(consumer);
        text.print().setParallelism(1);
        env.execute("StreamingKafkaSource");
    }
}
