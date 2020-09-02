package FlinkBatch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) {
        ExecutionEnvironment env  = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("path/to/file");
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);
        counts.writeAsCsv("outputPath", "\n", " ");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0){
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
