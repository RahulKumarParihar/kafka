package com.rahulkumarparihar.kafka;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer extends BaseClass {
    private final String topic;
    private final String groupId;
    private final String openSearchUrl;
    Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public Consumer(String topic, String groupId, String openSearchUrl) {
        this.topic = topic;
        this.groupId = groupId;
        this.openSearchUrl = openSearchUrl;
    }

    private KafkaConsumer create() {
        Properties properties = getConsumerProperties();
        // set groupId
        properties.setProperty("group.id", groupId);

        // set auto offset -- none/earliest/latest
        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty("enable.auto.commit", "false");

        return new KafkaConsumer<String, String>(properties);
    }

    private RestHighLevelClient createOpenSearchClient() {
        URI connUri = URI.create(openSearchUrl);

        return new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort())));
    }

    public void poll(final long consumeAfterEverySeconds) {
        KafkaConsumer<String, String> consumer = create();
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // get the reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(Collections.singleton(topic));

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else {
                log.info("The Wikimedia Index already exits");
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(consumeAfterEverySeconds));

                BulkRequest bulkRequest = new BulkRequest();

                log.info("Records Consumed: " + records.count());

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // send the record into OpenSearch

                        // define an ID using Kafka Record coordinates
                        String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {

                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        log.info("Inserted " + bulkResponse.getItems().length + " record(s).");

                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // commit offsets after the batch is consumed
                        consumer.commitSync();
                        log.info("Offsets have been committed!");
                    }

                }
            }
        } catch (WakeupException exception) {
            log.info("Consumer is starting to shut down");
        } catch (Exception exception) {
            log.error("Unexpected exception in the consumer", exception);
        } finally {
            // close the consumer and commit the offsets
            consumer.close();
        }
    }
}
