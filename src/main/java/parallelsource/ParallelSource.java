package parallelsource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @program: WordCountDemo
 * @description: 并行流source
 * @author: 李沛隆21081020
 * @create: 2021-10-09 09:23
 */
public class ParallelSource implements ParallelSourceFunction<Long> {
    private boolean isRunning = true;
    private long count = 1L;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
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
