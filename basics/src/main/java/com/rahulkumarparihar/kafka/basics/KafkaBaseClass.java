package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public abstract class KafkaBaseClass {
    private static void setConnectionProperties(Properties properties) {
        // connect to localhost
//        properties.setProperty("bootstrap-server","127.0.0.1:9092");

        // connect to conductor host
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "");
        properties.setProperty("sasl.mechanism", "PLAIN");
    }

    protected static Properties getProducerProperties() {
        Properties properties = new Properties();

        setConnectionProperties(properties);

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }
}

