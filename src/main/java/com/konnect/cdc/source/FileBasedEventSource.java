package com.konnect.cdc.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileBasedEventSource implements EventSource {
    private final String filePath;
    private BufferedReader reader;

    public FileBasedEventSource(String filePath) throws EventSourceException {
        this.filePath = filePath;
        try {
            this.reader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {
            throw new EventSourceException("Failed to open file: " + filePath, e);
        }
    }

    @Override
    public Iterator<String> loadData() {
        return new Iterator<String>() {
            private String nextLine = null;

            @Override
            public boolean hasNext() {
                if (nextLine != null) {
                    return true;
                }
                try {
                    nextLine = reader.readLine();
                    return nextLine != null;
                } catch (IOException e) {
                    throw new EventSourceException("Error reading events", e);
                }
            }

            @Override
            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                String line = nextLine;
                nextLine = null;
                return line;
            }
        };
    }

    @Override
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            // Log or handle as appropriate
        }
    }

    @Override
    public EventSourceMetadata getSourceMetadata() {
        return new EventSourceMetadata(filePath, "File based event source");
    }
}
