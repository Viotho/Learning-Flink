package FlinkStreaming.Source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// RichParallelFounction额外提供open()方法与close()方法打开与关闭外部数据源
public class RichParallelSource extends RichParallelSourceFunction<Long> {
    private long count = 1L;
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

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Open......");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
