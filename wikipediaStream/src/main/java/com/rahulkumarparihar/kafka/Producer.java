package com.rahulkumarparihar.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer extends BaseClass {
    private final String topic;
    private final String url;

    public Producer(String topic, String url) {
        this.topic = topic;
        this.url = url;
    }

    private KafkaProducer create() {
        Properties properties = getProduerProperties();

        return new KafkaProducer<String, String>(properties);
    }

    public void sendMessage(final long produceForMinutes) throws InterruptedException {
        KafkaProducer<String, String> producer = create();

        EventHandler eventHandler = new WikipediaChangeEventHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        // start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(produceForMinutes);
    }
}
