package edu.supmti.kafka;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Scanner;

public class WordProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Connexion aux 3 brokers du cluster
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "WordCount-Topic";
        Scanner scanner = new Scanner(System.in);

        System.out.println("Tapez vos mots (Entrée pour envoyer, 'exit' pour quitter) :");
        while (true) {
            String line = scanner.nextLine();
            if ("exit".equalsIgnoreCase(line)) break;
            
            for (String word : line.split("\\W+")) {
                producer.send(new ProducerRecord<>(topic, word, word));
            }
        }
        producer.close();
        scanner.close();
    }
}