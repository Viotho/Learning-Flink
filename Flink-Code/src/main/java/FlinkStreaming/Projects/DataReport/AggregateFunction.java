package FlinkStreaming.Projects.DataReport;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;

public class AggregateFunction implements WindowFunction<Tuple3<Long, String, String>, Tuple4<String, String, String, Long>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<Long, String, String>> input, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
        String type = tuple.getField(0).toString();
        String area = tuple.getField(1).toString();

        Iterator<Tuple3<Long, String, String>> iterator = input.iterator();
        ArrayList<Long> arrayList = new ArrayList<>();

        long count = 0;
        while (iterator.hasNext()){
            Tuple3<Long, String, String> next = iterator.next();
            arrayList.add(next.f0);
            count++;
            System.out.println(Thread.currentThread().getId() + ", window triggered, number of data items: " + count);
            Collections.sort(arrayList);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = sdf.format(new Date(arrayList.get(arrayList.size() - 1)));

            Tuple4<String, String, String, Long> result = new Tuple4<>(time, type, area, count);
            collector.collect(result);
        }
    }
}
