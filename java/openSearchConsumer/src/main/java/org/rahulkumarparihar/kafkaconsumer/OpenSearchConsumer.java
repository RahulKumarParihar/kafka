package org.rahulkumarparihar.kafkaconsumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    public static final String INDEX_NAME = "wikimedia";

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // get the reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, lets exit by calling consumer.wake()...");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try (openSearchClient; consumer) {
            // Create open search index
            if (!openSearchClient.indices().exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndex = new CreateIndexRequest(INDEX_NAME);
                openSearchClient.indices().create(createIndex, RequestOptions.DEFAULT);

                log.info("wikimedia Index is created.");
            } else {
                log.info("wikimedia Index already exists");
            }

            // consume for kafka
            String topic = "wikipedia.open.search.topic";

            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    String id = extractId(record.value());

                    try {
                        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        // Not sending request one by one
                        // IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

                        //log.info("Added record with ID " + id +" to bulk request");

                    } catch (Exception ex) {

                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    log.info("Inserted " + bulkResponse.getItems().length + " record(s) to openSearch");

                    consumer.commitSync();
                    log.info("Committed offset");
                }
            }
        } catch (WakeupException exception) {
            log.info("Consumer is shutting down");
        } catch (Exception exception) {
            log.error("Unexpected exception in the consumer", exception);
        } finally {
            consumer.close();
            openSearchClient.close();
            log.info("Consumer is gracefully shutdown");
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject().get("meta")
                .getAsJsonObject().get("id")
                .getAsString();
    }

    private static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200/";

        URI connUri = URI.create(connString);

        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // Rest client without security
            return new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())));
        } else {
            // Rest client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            return new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        // set consumer deserializer
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // set groupId
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "open_search_consumer");

        // set auto offset -- none/earliest/latest
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<String, String>(properties);
    }
}
