package FlinkStreaming.Projects.ETL;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class DataProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties  = new Properties();
        properties.put("bootstrap.servers", "kafka-01:9092, kafka-02:9092");
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        String topic = "allData";
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        while (true){
            String message = "{\"dt\":\"" + getCurrentTime() + "\", \"countryCode\":\"" + getCountryCode() +
                    "\",\"data\":[{\"type\":\"" + getRandomType() + "\", \"score\":" + getRandomScore() +
                    ",\"level\":\"" + getRandomLevel() + "\"}, {\"type\":\"" + getRandomType() + "\",\"score\":" +
                    getRandomScore() + ", \"level\":\"" + getRandomLevel() + "\"}]}";

            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(2000);

        }
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getCountryCode(){
        String[] types = {"US", "PK", "KW", "SA", "IN"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomType(){
        String[] types = {"s1", "s2", "s3","s4", "s5" };
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static double getRandomScore(){
        double[] types = {0.3, 0.2, 0.1, 0.5, 0.8};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
    public static String getRandomLevel(){
        String[] types = {"A+", "A", "B", "C", "D"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
