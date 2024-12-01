package com.konnect.cdc.source;


import java.util.Iterator;

/**
 * Event Source interface today we have file tomorrow we could have some other source like a DB or kafka itself
 */
public interface EventSource {
    /**
     * Returns an iterator of CDC events.
     * This allows for lazy loading and support of different event sources.
     */
    Iterator<String> loadData() throws EventSourceException;

    void close();

    public EventSourceMetadata getSourceMetadata();

}
