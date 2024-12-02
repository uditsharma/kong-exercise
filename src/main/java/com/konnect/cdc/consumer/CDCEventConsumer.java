package com.konnect.cdc.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
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

public class CDCEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CDCEventConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestHighLevelClient client;

    //TODO
    //  -- enable retries while indexing
    // -- handle stream processing errors better
    // -- add tests using test containers

    public CDCEventConsumer() {
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    public void processEvents(String inputTopic, String openSearchIndex) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cdc-event-processor");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "cdc-event-processor"); // append to app resources
        // #partition == # of tasks
        // each thread will get number of tasks to process from
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Runtime.getRuntime().availableProcessors());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
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
                        String docId = after.get("key").asText();
                        // Create OpenSearch client
                        // Index document
                        IndexRequest indexRequest = new IndexRequest(openSearchIndex)
                                .id(docId)
                                .source(objectMapper.writeValueAsString(after), XContentType.JSON);

                        client.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("Indexed document: {}", eventNode);
                    } catch (IOException ex) {
                        logger.error("Error processing event {}", value, ex);
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((exception) -> {
            // Log the exception
            logger.error("Uncaught exception in Kafka Streams: ", exception);

            if (exception instanceof RuntimeException) {
                // Return SHUTDOWN_APPLICATION for critical errors
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
            // Default behavior: replace thread
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.setStateListener((newState, oldState) -> {
            int streamState = newState.ordinal();
            //TODO we can even send this state to a monitoring system
            // like Prometheus and log how much time it took to recover or if it failed
            if (newState == org.apache.kafka.streams.KafkaStreams.State.REBALANCING) {
                logger.info("Stream state changed from {} to {}", oldState, newState);
            } else if (newState.equals(org.apache.kafka.streams.KafkaStreams.State.RUNNING)) {
                logger.info("Stream state changed from {} to {}", oldState, newState);
            } else if (newState.equals(org.apache.kafka.streams.KafkaStreams.State.ERROR)) {
                logger.info("Stream state changed from {} to {}", oldState, newState);
            }
        });
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