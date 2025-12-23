package edu.supmti.kafka;

import java.util.Arrays;
import java.util.Properties;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EventConsumer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }
        String topicName = args[0].toString();
        
        // Configuration du Consommateur [cite: 403-411]
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test"); // Groupe de consommateurs
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // Souscription au topic [cite: 413]
        consumer.subscribe(Arrays.asList(topicName));
        System.out.println("Souscris au topic " + topicName);
        
        // Boucle infinie pour lire les messages [cite: 416-423]
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", 
                    record.offset(), record.key(), record.value());
            }
        }
    }
}