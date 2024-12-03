package com.konnect.cdc.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.konnect.cdc.CDCEvent;

public interface Sink {
    boolean sink(CDCEvent document);
}
