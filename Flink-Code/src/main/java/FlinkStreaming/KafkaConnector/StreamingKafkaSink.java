package FlinkStreaming.KafkaConnector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class StreamingKafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("hadoop-01", 9001, "\n");
        String brokerList = "hadoop-02:9092";
        String topic = "topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerList);
        properties.setProperty("transaction.timeout.ms", 60000 * 15 + "");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(topic, (KafkaSerializationSchema<String>) new SimpleStringSchema(), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        text.addSink(producer);
        env.execute("StreamingKafkaSink");
    }
}
