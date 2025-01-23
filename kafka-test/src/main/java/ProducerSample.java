

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;



public class ProducerSample {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try{
//            Send 5 messages to the first topic
            for (int i = 0; i < 5; i++) {
                String key = "key-" + i;
                String value = "Message-" + i;

                //Create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", key, value);

                //Send the message asynchronously
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("Sent message: (key=%s, value=%s) to partition=%d, offset=%d%n",
                        key, value, metadata.partition(), metadata.offset());
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
