package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producers extends KafkaBaseClass {
    private static final Logger _log = LoggerFactory.getLogger(Producers.class.getSimpleName());

    public static void main(String[] args) {
        _log.info("Hello from producer!");

        // create producer properties
        Properties properties = getProducerProperties();

        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("third_topic", "Hello from producer");

        // send data
        kafkaProducer.send(record);

        // flush and close the producers
        // tells the producer to send all data and block until done --synchronous
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
