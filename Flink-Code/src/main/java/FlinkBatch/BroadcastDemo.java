package FlinkBatch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BroadcastDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<Tuple2<String, Integer>>();
        broadData.add(new Tuple2<String, Integer>("zs", 18));
        broadData.add(new Tuple2<String, Integer>("ls", 20));
        broadData.add(new Tuple2<String, Integer>("ww", 17));
        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        DataSet<HashMap<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> res = new HashMap<String, Integer>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        DataSource<String> data = env.fromElements("zs", "ls", "ww");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadcastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadcastMap = getIterationRuntimeContext().getBroadcastVariable("BroadcastMapName");
                for (HashMap map : broadcastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadcast, "BroadcastMapName");
        result.print();
    }
}
