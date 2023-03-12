package com.rahulkumarparihar.kafka.wikimediaProducer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // connect to local server
        //properties.setProperty("ProducerConfig.BOOTSTRAP_SERVERS_CONFIG", "127.0.0.1:9202");

        // connect to ssl server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");

        // set producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // for Kafka < 3.0 to make safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(-1)); // -1 or all represent the same value
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.toString(true));
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(120000));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));


        // create producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "wikipedia.open.search.topic";

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        TimeUnit.SECONDS.sleep(10);
    }
}
