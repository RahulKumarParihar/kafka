package com.rahulkumarparihar.kafka;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class BaseClass {
    private final String bootstrapServer = "127.0.0.1:9092";

    /**
     * @return properties of consumer
     */
    public Properties getConsumerProperties() {
        Properties properties = getProperties();

        // set serializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        return properties;
    }

    /**
     * @return properties of producer
     */
    public Properties getProduerProperties() {
        Properties properties = getProperties();

        // set serializer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }

    private Properties getProperties() {
        Properties properties = new Properties();

        // set bootstrap server
        properties.setProperty("bootstrap.servers", bootstrapServer);

        return properties;
    }
}
