package com.konnect.cdc.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OpenSearchSink implements Sink {
    private static final Logger logger = LoggerFactory.getLogger(OpenSearchSink.class);

    private final String openSearchIndex;
    private final RestHighLevelClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OpenSearchSink(String openSearchIndex) {
        this.openSearchIndex = openSearchIndex;
        this.client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    @Override
    public boolean sink(JsonNode document) {
        try {
            JsonNode after = document.get("after");
            String docId = after.get("key").asText();

            // Index document
            IndexRequest indexRequest = new IndexRequest(openSearchIndex)
                    .id(docId)
                    .source(objectMapper.writeValueAsString(after), XContentType.JSON);

            client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Indexed document: {}", document);
            return true;  // Success branch
        } catch (Exception ex) {
            logger.error("Error processing event {}", document, ex);
            return false;
        }
    }
}