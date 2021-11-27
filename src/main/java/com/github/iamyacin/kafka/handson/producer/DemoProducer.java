package com.github.iamyacin.kafka.handson.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DemoProducer {
    public static void main(String[] args) {
        //Define the bootstrapServers variable :
        String bootstrapServers = "localhost:9092";
        // Create the producer properties :
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the producer :
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //Create record :
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("MultiPartitionTopic", "Hello From Java App");
        // Send the record async :
        producer.send(record);
        producer.flush();
        //close producer :
        producer.close();
    }
}
