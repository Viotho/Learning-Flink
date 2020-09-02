package FlinkStreaming.Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StreamingWindowWatermark {
    public static void main(String[] args) throws Exception {
        int port = 9000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("hadoop-1", port, "\n");
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        DataStream<Tuple2<String, Long>> watermarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            final Long maxOutOfOrders =10000L;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrders);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("Key:" + element.f0 + ", Event-Time: [" + element.f1 + "|" + sdf.format(element.f1) + "], currentMaxTimestamp: [" +
                        currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "], Watermark: [" + getCurrentWatermark().getTimestamp() +
                        "|" + sdf.format(getCurrentWatermark().getTimestamp() + "]"));
                return timestamp;
            }
        });

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<>("Late-Data");
        SingleOutputStreamOperator<String> window = watermarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    // Sorts the elements in window for the correct order.
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input, Collector<String> collector) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrayList = new ArrayList<Long>();
                        Iterator<Tuple2<String, Long>> iter = input.iterator();
                        while (iter.hasNext()){
                            Tuple2<String, Long> next = iter.next();
                            arrayList.add(next.f1);
                        }
                        Collections.sort(arrayList);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + ", " + arrayList.size() + ", " + sdf.format(arrayList.get(0)) + "," + sdf.format(arrayList.get(arrayList.size() - 1)) +
                                ", " + sdf.format(timeWindow.getStart()) + ", " + sdf.format(timeWindow.getEnd());
                        collector.collect(result);
                    }
                });

        DataStream<Tuple2<String, Long>> sideOutput = window.getSideOutput(outputTag);
        sideOutput.print();
        window.print();
        env.execute("EventTime-Watermark");
    }
}
