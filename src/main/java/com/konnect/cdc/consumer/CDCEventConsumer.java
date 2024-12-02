package com.konnect.cdc.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.konnect.cdc.sink.OpenSearchSink;
import com.konnect.cdc.sink.Sink;
import org.apache.http.HttpHost;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class CDCEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CDCEventConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String appId;
    private final String inputTopic;
    private final String deadLetterTopic;
    private final Sink sink;

    //TODO
    //  -- enable retries while indexing
    // -- handle stream processing errors better
    // -- add tests using test containers

    public CDCEventConsumer(String appId, String inputTopic, String deadLetterTopic, Sink sink) {
        this.appId = appId;
        this.inputTopic = inputTopic;
        this.deadLetterTopic = deadLetterTopic;
        this.sink = sink;
    }

    public void consumeEvents() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-" + UUID.randomUUID()); // append to app resources
        // #partition == # of tasks
        // each thread will get number of tasks to process from
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Runtime.getRuntime().availableProcessors());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // log and continue in case bad message come. we could even branch out to a dead letter queue
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        // Reliability Configurations


        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Consume events from Kafka topic
        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        // Split stream into success and failure using named predicates
        Map<String, KStream<String, String>> branches = inputStream.split(Named.as("opensearch-"))
                .branch((key, value) -> {
                    try {
                        JsonNode eventNode = objectMapper.readTree(value);
                        return sink.sink(eventNode);
                    } catch (Exception ex) {
                        logger.error("Error processing event {}", value, ex);
                        return false;
                    }
                }, Branched.as("success")).defaultBranch(Branched.as("failure"));


        // Get the success and failure streams
        KStream<String, String> successStream = branches.get("opensearch-success");

        KStream<String, String> failureStream = branches.get("opensearch-failure");

// For debugging
        branches.forEach((key, value) -> {
            logger.info("Branch key: {}", key);
        });
        // Handle failed records and push to dead letter queue so that we can debug and reproduce the issue
        failureStream
                .mapValues(this::createDLQRecord)
                .to(deadLetterTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // setup stream handlers, such as exception handlers, state listeners, shutdown hooks
        setupStreamHandlers(streams);
        // Start the Kafka Streams application
        startStreams(streams);
    }

    private String createDLQRecord(String originalValue) {
        try {
            DeadLetterQueueRecord dlqRecord = new DeadLetterQueueRecord(
                    originalValue,
                    System.currentTimeMillis(),
                    appId
            );
            return objectMapper.writeValueAsString(dlqRecord);
        } catch (JsonProcessingException e) {
            logger.error("Error creating DLQ record", e);
            return originalValue;
        }
    }


    private void setupStreamHandlers(KafkaStreams streams) {
        // Exception Handler
        streams.setUncaughtExceptionHandler((exception) -> {
            logger.error("Uncaught exception in Kafka Streams: ", exception);

            if (exception instanceof RuntimeException) {
                return StreamsUncaughtExceptionHandler
                        .StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
            return StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // State Listener
        streams.setStateListener((newState, oldState) -> {
            logger.info("Stream state changed from {} to {}", oldState, newState);

            if (newState == KafkaStreams.State.ERROR) {
                // Could add metrics/alerting here
                logger.error("Streams entered ERROR state!");
            }
        });

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down streams application...");
            streams.close(Duration.ofSeconds(10));
        }));
    }

    private void startStreams(KafkaStreams streams) {
        try {
            streams.start();
        } catch (Exception e) {
            logger.error("Error starting streams", e);
            throw new RuntimeException("Failed to start Kafka Streams", e);
        }
    }

    public static void main(String[] args) {
        // Example usage with file source
        if (args.length < 1) {
            System.out.println("Usage: CDCEventConsumer <kafka_topic>");
            System.exit(1);
        }

        Sink sink = new OpenSearchSink("konnect-entities");
        String topic = args[0];
        CDCEventConsumer consumer = new CDCEventConsumer("OpenSearchIndexer", topic, "dead-letter-queue", sink);
        consumer.consumeEvents();
    }
}