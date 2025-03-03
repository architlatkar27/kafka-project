package org.example;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class OpensearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());
        // create opensearch client
        RestHighLevelClient osClient = createOpenSearchClient();

        // create kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer("consumer-opensearch-demo");

        // create index in opensearch
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, lets exit by calling consumer.wakeup()");
                consumer.wakeup();
                // join the main thread to allow exec in main
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        try (osClient; consumer) {
            boolean exists = osClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest req = new CreateIndexRequest("wikimedia");
                createOpenSearchClient().indices().create(req, RequestOptions.DEFAULT);
                log.info("wikimedia index created");
            } else {
                log.info("wikimedia index already exists");
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received "+recordCount+" record(s) in poll");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record: records) {
                    try {
                        // make consumer idempotent by having an id of the message
                        String id = extractId(record);

                        IndexRequest indexRequest = new IndexRequest("wikimedia");
                        indexRequest.source(record.value(), XContentType.JSON).id(id);
                        bulkRequest.add(indexRequest);
//                        log.info("Inserted 1 document into opensearch "+response.getId());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = osClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted "+bulkResponse.getItems().length + " record(s)");

                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        log.error("couldn't even sleep for 2s");
                    }
                    // manual offest commit
                    consumer.commitSync();
                    log.info("offsets have been commited");
                }

            }
        } catch (WakeupException e) {
            log.info("consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("unexpected exception");
        } finally {
            consumer.close();
            osClient.close();
        }
    }

    private static String extractId(ConsumerRecord<String, String> record) {
        return JsonParser.parseString(record.value()).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }
}
