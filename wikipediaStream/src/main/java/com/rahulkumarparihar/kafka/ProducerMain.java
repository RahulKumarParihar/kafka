package com.rahulkumarparihar.kafka;

public class ProducerMain {
    public static void main(String[] args) throws InterruptedException {
        String topic = "wikipedia_open_search_topic";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Producer producer = new Producer(topic, url);
        producer.sendMessage(1);
    }
}
