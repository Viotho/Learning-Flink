package FlinkStreaming.Projects.DataReport;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaProduceDataReport {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "");
        properties.put("key.serializer", "");
        properties.put("value.serializer", "");
        String topic = "auditLog";

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        while (true){
            String message =
                    "{\"dt\":\""+ getCurrentTime() + "\",\"type\":\"" + getRandomType()+"\",\"username\":\"" +
                            getRandomUsername() + "\",\"area\":\"" + getRandomArea() + "\"}";
            System.out.println(message);
            producer.send(new ProducerRecord<String, String>(topic, message));
            Thread.sleep(1000);
        }
    }

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    public static String getRandomArea(){
        String[] types = {"AREA_US", "AREA_AR", "AREA_IN", "AREA_ID"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomType(){
        String[] types = {"shelf", "unshelf", "black", "child_chelf", "child_unshelf"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }

    public static String getRandomUsername(){
        String[] types = {"shenhe1" ,"shenhe2", "shenhe3", "shenhe4", "shenhe5"};
        Random random = new Random();
        int i = random.nextInt(types.length);
        return types[i];
    }
}
