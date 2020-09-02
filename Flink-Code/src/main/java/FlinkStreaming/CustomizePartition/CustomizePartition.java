package FlinkStreaming.CustomizePartition;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomizePartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("Num of Partitions" + numPartitions);
        if (key % 2 == 0){
            return 0;
        }
        else {
            return 1;
        }
    }
}
