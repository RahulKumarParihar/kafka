package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumers extends KafkaBaseClass {
    private static final Logger _log = LoggerFactory.getLogger(Consumers.class.getSimpleName());

    public static void main(String[] args) {
        _log.info("Hello from consumer!");

        // create consumer properties
        Properties properties = getConsumerProperties("my-application-group");

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        kafkaConsumer.subscribe(List.of("third_topic"));

        while (true) {
            _log.info("Polling");

            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                _log.info("Key: " + record.key() + " Value: " + record.value());
                _log.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
