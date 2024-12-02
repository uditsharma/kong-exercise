package com.konnect.cdc.producer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.konnect.cdc.source.EventSource;
import com.konnect.cdc.source.EventSourceMetadata;
import com.konnect.cdc.source.FileBasedEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class CDCEventProducer {
    private static final Logger logger = LoggerFactory.getLogger(CDCEventProducer.class);
    private final KafkaProducer<String, String> producer;
    private final EventSource eventSource;
    private final String topicName;

    // Counter for successful and failed events
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);

    public CDCEventProducer(EventSource eventSource, String topicName) {
        this.eventSource = eventSource;
        this.topicName = topicName;
        // Kafka Producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "flexible-cdc-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // this means only leader can ack and we can be done with this, for more stronger needs we can increase this
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // Infinite retries
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500); // waiit for 500ms before sending a batch
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 100); // 100 records per batch
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer, max size of the buffer
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Efficient compression
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 50); // 50ms backoff
        producer = new KafkaProducer<>(props);
    }

    public void produceEvents() {

        // Create Kafka Producer
        try {// Iterate through events
            Iterator<String> eventIterator = eventSource.loadData();
            int eventCount = 0;

            while (eventIterator.hasNext()) {
                String event = eventIterator.next();
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode eventJson = mapper.readTree(event);
                    JsonNode after = eventJson.get("after");
                    // using this key to distribute, ideally i want to use the document key
                    String key = after.get("key").asText();
                    // Create a ProducerRecord for each event
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            topicName,
                            key, // Let Kafka generate key
                            event // JSON event as value
                    );

                    // Send the record to Kafka
                    producer.send(record, (recordMetadata, exception) -> {
                        if (exception != null) {
                            failureCount.incrementAndGet();
                            logger.error("Error sending event", exception);
                        } else {
                            successCount.incrementAndGet(); // this could be failure metrics
                            logger.debug("Sent event to partition {} with offset {}",
                                    recordMetadata.partition(),
                                    recordMetadata.offset());
                        }
                    });
                    eventCount++;
                } catch (Exception e) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            topicName,
                            UUID.randomUUID().toString(), // Let Kafka generate key
                            event // JSON event as value
                    );
                    producer.send(record);
                    failureCount.incrementAndGet();
                    logger.error("Error processing event", e);
                }
            }

            // Flush and ensure all records are sent
            producer.flush();
            logger.info("Finished producing {} events to topic {} from source: {}", eventCount, topicName, eventSource.getSourceMetadata().getSourceId());
        } finally {
            // Ensure source is closed
            eventSource.close();
        }
    }

    public static void main(String[] args) {
        // Example usage with file source
        if (args.length < 2) {
            System.out.println("Usage: CDCEventProducer <source_type> <source_path> <kafka_topic>");
            System.exit(1);
        }
        try {
            EventSource eventSource;
            String sourceType = args[0];
            String sourcePath = args[1];
            String topicName = args[2];
            // this can be a factory
            switch (sourceType.toUpperCase()) {
                case "FILE":
                    eventSource = new FileBasedEventSource(sourcePath);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported source type: " + sourceType);
            }
            CDCEventProducer producer = new CDCEventProducer(eventSource, topicName);
            producer.produceEvents();
        } catch (Exception e) {
            logger.error("Error producing events", e);
            System.exit(1);
        }
    }
}
