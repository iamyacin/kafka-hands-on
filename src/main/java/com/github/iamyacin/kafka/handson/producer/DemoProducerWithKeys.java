package com.github.iamyacin.kafka.handson.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DemoProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Define a logger for our class :
        final Logger logger = LoggerFactory.getLogger(DemoProducerWithCallBack.class);

        //Define the bootstrapServers variable :
        String bootstrapServers = "localhost:9092";

        // Create the producer properties :
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer :
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 0;
        while(i<21){
            //Set the record attribute:
            String topic = "MultiPartitionTopic";
            String value = "Hello From Java App"+Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            //Create record :
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("key : "+key); //The same key go to the same partition every fucking time (key X to the partition Y)

            // Send the record async :
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        logger.info("Message is successfully sent."+"\n"+
                                "Topic: "+recordMetadata.topic()+"\n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "Timestamp: "+recordMetadata.timestamp());
                    }else {
                        logger.error("Error while producing message: " + e.getMessage());
                    }
                }
            }).get(); // this get is just for testing and showing the result, don't do this in production!
            i++;
        }
        producer.flush();
        //close producer :
        producer.close();

    }
}
