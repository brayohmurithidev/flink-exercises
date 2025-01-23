import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerSample {
    public static void main(String[] args) {
        //create configurations
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "kafka-test");
        props.put("auto.offset.reset", "earliest");

        //create consumer.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //subscribe to the consumer
        consumer.subscribe(Collections.singletonList("first-topic")); //can also user Arrays.asList(topic) - for multiple topics

        System.out.println("Consumer started. Waiting for messages...");

        try {
            while (true) {
                // Poll for messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: (key=%s, value=%s) from partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
