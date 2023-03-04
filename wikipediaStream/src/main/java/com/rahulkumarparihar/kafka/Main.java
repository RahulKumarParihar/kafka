package com.rahulkumarparihar.kafka;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String topic = "wikimedia.recentChange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        Producer producer = new Producer(topic, url);
        producer.sendMessage(1);
    }
}
