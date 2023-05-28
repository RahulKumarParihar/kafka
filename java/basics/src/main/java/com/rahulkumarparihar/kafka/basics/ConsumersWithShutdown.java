package com.rahulkumarparihar.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumersWithShutdown extends KafkaBaseClass {
    private static final Logger _log = LoggerFactory.getLogger(ConsumersWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        _log.info("Hello from consumer!");

        // create consumer properties
        Properties properties = getConsumerProperties("my-application-group");

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // get the reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                _log.info("Detected a shutdown, lets exit by calling consumer.wake()...");

                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
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
        } catch (WakeupException exception) {
            _log.info("Consumer is shutting down");
        } catch (Exception exception) {
            _log.error("Unexpected exception in the consumer", exception);
        } finally {
            // close the consumer and commit the offsets
            kafkaConsumer.close();
            _log.info("Consumer is gracefully shutdown");
        }
    }
}