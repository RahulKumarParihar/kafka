package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public abstract class KafkaBaseClass {
    private static void setConnectionProperties(Properties properties) {
        // connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    }

    protected static Properties getProducerProperties() {
        Properties properties = new Properties();

        setConnectionProperties(properties);

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }

    protected static Properties getConsumerProperties(String groupId) {
        Properties properties = new Properties();

        setConnectionProperties(properties);

        // set consumer deserializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // set groupId
        properties.setProperty("group.id", groupId);

        // set auto offset -- none/earliest/latest
        properties.setProperty("auto.offset.reset", "earliest");

        return properties;
    }
}

