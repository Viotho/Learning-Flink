package FlinkStreaming.Sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class StreamingDemoToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("Hadoop-o1", 9000, "\n");
        DataStream<Tuple2<String, String>> OneWordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<String, String>("one_words", value);
            }
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("Hadoop-02").setPort(6379).build();
        RedisSink<Tuple2<String, String>>  redisSink = new RedisSink<>(config, new MyRedisMaper());
        OneWordsData.addSink(redisSink);
        env.execute("StreamingDemoToRedis");
    }

    public static class MyRedisMaper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return  data.f1;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
    }
}
