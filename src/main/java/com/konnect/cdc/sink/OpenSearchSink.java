package com.konnect.cdc.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.konnect.cdc.CDCEvent;
import org.apache.http.HttpHost;
import org.opensearch.action.delete.DeleteRequest;
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
    public boolean sink(CDCEvent document) {
        try {
            // Extract document ID
            if (document.getDocId().isPresent()) {
                if (document.isDelete()) {
                    // Delete document
                    String docId = document.getDocId().get();
                    client.delete(new DeleteRequest(openSearchIndex, docId), RequestOptions.DEFAULT);
                    logger.info("Deleted document: {}", document);
                } else if (document.isCreate() || document.isUpdate()) {
                    // Index document if document to index is present
                    if (document.getDocumentToIndex().isPresent()) {
                        IndexRequest indexRequest = new IndexRequest(openSearchIndex)
                                .id(document.getDocId().get())
                                .source(objectMapper.writeValueAsString(document.getDocumentToIndex().get()), XContentType.JSON);
                        client.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info("Indexed document: {}", document);
                    } else {
                        logger.warn("Document to index is missing: {}", document);
                    }
                } else {
                    logger.warn("Unsupported operation: {}", document);
                }
            }
            return true;
        } catch (Exception ex) {
            logger.error("Error processing event {}", document, ex);
            return false;
        }
    }
}