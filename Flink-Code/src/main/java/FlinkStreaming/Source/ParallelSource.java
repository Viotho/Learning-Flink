package FlinkStreaming.Source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class ParallelSource implements ParallelSourceFunction<Long> {
    private long count= 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
