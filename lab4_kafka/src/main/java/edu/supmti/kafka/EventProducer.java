package edu.supmti.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
    public static void main(String[] args) throws Exception {
        // Vérification de l'argument (nom du topic)
        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }
        String topicName = args[0].toString();

        // Configuration du Producteur [cite: 288-299]
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Adresse du broker
        props.put("acks", "all"); // Acquittement complet
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Création de l'instance Producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Envoi des messages (0 à 9) [cite: 301-303]
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
        }
        
        System.out.println("Message envoyé avec succès");
        producer.close();
    }
}