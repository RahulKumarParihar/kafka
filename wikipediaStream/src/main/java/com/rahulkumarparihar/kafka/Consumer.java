package com.rahulkumarparihar.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class Consumer extends BaseClass {
    public KafkaConsumer create() {
        Properties properties = getConsumerProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        return consumer;
    }
}
