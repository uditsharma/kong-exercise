package com.konnect.cdc;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

public class CDCEvent {
    private final JsonNode event;

    public CDCEvent(JsonNode event) {
        this.event = event;
    }


    public JsonNode getEvent() {
        return event;
    }


    public Optional<String> getDocId() {
        if (event.has("after")) {
            return Optional.ofNullable(event.get("after").get("key").asText());
        } else if (event.has("before")) {
            return Optional.ofNullable(event.get("before").get("key").asText());
        }
        return Optional.empty();
    }


    public Optional<JsonNode> getDocumentToIndex() {
        // seems like file have key -> value format in after and before
        if (event.has("after")) {
            return Optional.ofNullable(event.get("after").get("value"));
        } else if (event.has("before")) {
            return Optional.ofNullable(event.get("before").get("value"));
        }
        return Optional.empty();
    }

    public boolean isCreate() {
        return "c".equals(event.get("op").asText());
    }

    public boolean isDelete() {
        return "d".equals(event.get("op").asText());
    }

    public boolean isUpdate() {
        return "u".equals(event.get("op").asText());
    }
}
