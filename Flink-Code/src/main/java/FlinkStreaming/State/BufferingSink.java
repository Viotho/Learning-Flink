package FlinkStreaming.State;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    private final int threshold;
    private transient ListState<Tuple2<String, Integer>> checkpointedState;
    private List<Tuple2<String, Integer>> bufferedElement;

    public BufferingSink(int threshold){
        this.threshold = threshold;
        this.bufferedElement = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value) throws Exception {
        bufferedElement.add(value);
        if (bufferedElement.size() == threshold){
            for (Tuple2<String, Integer> element : bufferedElement) {
                // Send To Sink
            }
            bufferedElement.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<Tuple2<String, Integer>>("buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()){
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElement.add(element);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElement) {
            checkpointedState.add(element);
        }
    }
}
