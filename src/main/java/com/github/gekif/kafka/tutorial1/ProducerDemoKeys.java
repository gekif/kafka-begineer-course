package com.github.gekif.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootStrapServers = "127.0.0.1:9092";

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // Create The Producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i ++) {
            // Create a Producer Record

            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);


            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // Log The Key


            // Send Data - Asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute Every Time A Record Is Successfully Sent Or An Exception Is Thrown
                    if (e == null) {
                        // The Record Was Successfully Sent
                        logger.info("Received New Metadata: \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error While Producing", e);
                    }
                }
            }).get(); // Block the .send() to make it synchronous - don't do this in production!
        }

        // Flush Data
        producer.flush();

        // Flush and Close Producer
        producer.close();

    }
}
