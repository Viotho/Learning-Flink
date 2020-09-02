package FlinkStreaming.Projects.ETL;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class DataClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        // Checkpoint Config
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String topic = "allData";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
        properties.setProperty("group.id", "con1");
        final FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);

        DataStreamSource<String> data = env.addSource(consumer);
        DataStream<HashMap<String, String>> mapData = env.addSource(new RedisSource()).broadcast();
        DataStream<String> resData = data.connect(mapData).flatMap(new CoFlatMapFunction<String, HashMap<String, String>, String>() {
            private HashMap<String, String> allMap = new HashMap<>();

            @Override
            public void flatMap1(String value, Collector<String> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String dt = jsonObject.getString("dt");
                String countryCode = jsonObject.getString("countryCode");
                String area = allMap.get(countryCode);

                JSONArray jsonArray = jsonObject.getJSONArray("data");
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                    jsonObject1.put("area", area);
                    jsonObject1.put("dt", dt);
                    collector.collect(jsonObject1.toJSONString());
                }
            }

            @Override
            public void flatMap2(HashMap<String, String> value, Collector<String> collector) throws Exception {
                allMap = value;
            }
        });

        String outTopic = "allDataClean";
        Properties outProperties = new Properties();
        outProperties.setProperty("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
        outProperties.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(outTopic, new SimpleStringSchema(), outProperties);
        resData.addSink(producer);
        env.execute("DataClean");
    }
}
