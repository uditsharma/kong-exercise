package com.konnect.cdc.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class CDCEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CDCEventConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestHighLevelClient client;

    public CDCEventConsumer() {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    public void processEvents(String inputTopic, String openSearchIndex) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cdc-event-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Reliability Configurations


        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Consume events from Kafka topic
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .foreach((key, value) -> {
                    try {
                        // Parse CDC event
                        JsonNode eventNode = objectMapper.readTree(value);
                        JsonNode after = eventNode.get("after");
                        // Create OpenSearch client
                        // Index document
                        IndexRequest indexRequest = new IndexRequest(openSearchIndex)
                                .id(UUID.randomUUID().toString())
                                .source(objectMapper.writeValueAsString(after), XContentType.JSON);

                        client.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("Indexed document: {}", eventNode);
                    } catch (IOException e) {
                        logger.error("Error processing event", e);
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Start the Kafka Streams application
        streams.start();
    }

    public static void main(String[] args) {
        // Example usage with file source
        if (args.length < 1) {
            System.out.println("Usage: CDCEventConsumer <kafka_topic>");
            System.exit(1);
        }

        String topic = args[0];
        CDCEventConsumer processor = new CDCEventConsumer();
        processor.processEvents(topic, "konnect-entities");
    }
}