package com.konnect.cdc.source;

public class EventSourceMetadata {
    private final String sourceId;
    private final String description;

    public EventSourceMetadata(String sourceId, String description) {
        this.sourceId = sourceId;
        this.description = description;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getDescription() {
        return description;
    }
}
