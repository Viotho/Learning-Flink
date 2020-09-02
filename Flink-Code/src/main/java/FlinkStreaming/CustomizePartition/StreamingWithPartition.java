package FlinkStreaming.CustomizePartition;

import FlinkStreaming.Source.NoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingWithPartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Long> text = env.addSource(new NoParallelSource());

        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<Long>(value);
            }
        });

        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new CustomizePartition(), new KeySelector<Tuple1<Long>, Long>() {
            @Override
            public Long getKey(Tuple1<Long> value) throws Exception {
                return value.f0;
            }
        });
//        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new CustomizePartition(), 0);
        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("Current Thread ID:" + Thread.currentThread().getId() + ", Value:" + value);
                return value.getField(0);
            }
        });
        result.print().setParallelism(1);
        env.execute("StreamingDemoWithPartition");
    }
}
