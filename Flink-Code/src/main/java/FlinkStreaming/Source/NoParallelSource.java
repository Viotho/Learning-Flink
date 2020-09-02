package FlinkStreaming.Source;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class NoParallelSource implements SourceFunction<Long> {
    private long count = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
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
