package com.rahulkumarparihar.kafka;

public class ConsumerMain {
    public static void main(String[] args) {
        String topic = "wikipedia_open_search_topic";
        String groupId = "wikipedia_open_search_group";
        String openSearchUrl = "http://localhost:9200";

        Consumer consumer = new Consumer(topic, groupId, openSearchUrl);
        consumer.poll(3);
    }
}
