package edu.supmti.kafka;

import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class WordCountConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "wordcount-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("WordCount-Topic"));

        Map<String, Integer> counts = new HashMap<>();

        System.out.println("En attente de messages...");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                String word = record.value();
                counts.put(word, counts.getOrDefault(word, 0) + 1);
                System.out.println("Compte actuel : " + counts);
            }
        }
    }
}