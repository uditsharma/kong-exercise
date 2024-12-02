package com.konnect.cdc.consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DeadLetterQueueRecord {
    private final String originalMessage;
    private final long failureTimeInMillis;
    private final String sourceApplication;
}
