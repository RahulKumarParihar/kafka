package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducersWithKeys extends KafkaBaseClass {
    private static final Logger _log = LoggerFactory.getLogger(ProducersWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        _log.info("Hello from producer!");

        // create producer properties
        Properties properties = getProducerProperties();

        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            // sending 10 records
            for (int i = 0; i < 10; i++) {
                String topic = "third_topic";
                String key = "id_" + i;
                String value = "Hello from producer" + i;

                // create a producer record
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // send data
                kafkaProducer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes everytime a record successfully sent or an exception is thrown
                        if (exception == null) {
                            // the record was successfully sent
                            _log.info("Key: " + key + " Partition: " + metadata.partition());
                        } else {
                            _log.error("Error while producing", exception);
                        }
                    }
                });
            }
        }

        // flush and close the producers
        // tells the producer to send all data and block until done --synchronous
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}