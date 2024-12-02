package com.konnect.cdc.sink;

import com.fasterxml.jackson.databind.JsonNode;

public interface Sink {
    boolean sink(JsonNode document);
}
