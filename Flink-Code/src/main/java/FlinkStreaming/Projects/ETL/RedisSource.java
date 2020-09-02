package FlinkStreaming.Projects.ETL;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

public class RedisSource implements SourceFunction<HashMap<String, String>> {
    private  Logger logger = LoggerFactory.getLogger(RedisSource.class);
    private final long SLEEP_MILLiON = 60000;
    private boolean isRunning = true;
    private Jedis jedis = null;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {
        this.jedis = new Jedis("redis", 6379);
        HashMap<String, String> keyValueMap = new HashMap<>();
        while (isRunning){
            try {
                keyValueMap.clear();
                Map<String, String> areas = jedis.hgetAll("areas");
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] splits = value.split(",");
                    for (String split : splits) {
                        keyValueMap.put(split, key);
                    }
                }
                if (keyValueMap.size() > 0){
                    sourceContext.collect(keyValueMap);
                }
                else {
                    logger.warn("Data from Redis is empty.");
                }
                Thread.sleep(SLEEP_MILLiON);
            } catch (JedisConnectionException e){
                logger.error("The link is abnormal, please get the link again.", e.getCause());
                jedis = new Jedis("redis", 6379);
            } catch (Exception e) {
                logger.error("Data source is abnormal.", e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (jedis != null){
            jedis.close();
        }
    }
}
