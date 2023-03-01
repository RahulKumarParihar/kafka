package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducersWithCallbacks extends KafkaBaseClass {
    private static final Logger _log = LoggerFactory.getLogger(ProducersWithCallbacks.class.getSimpleName());

    public static void main(String[] args) {
        _log.info("Hello from producer!");

        // create producer properties
        Properties properties = getProducerProperties();

        // set batch size -- DON'T CHANGE THIS IS PRODUCTION ENVIRONMENT
        properties.setProperty("batch.size", "400");

        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // creating multiple batches
        for (int r = 0; r < 10; r++) {
            // sending 30 records
            for (int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>("third_topic", "Hello from producer" + i);

                // send data
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes everytime a record successfully sent or an exception is thrown
                        if (exception == null) {
                            // the record was successfully sent
                            _log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            _log.error("Error while producing", exception);
                        }
                    }
                });

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // flush and close the producers
        // tells the producer to send all data and block until done --synchronous
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}