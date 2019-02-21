package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.sink.OMSQueue;

public abstract class DataEntry {

    public DataEntry(Long timestamp,
                     EntryType entryType,
                     OMSQueue queue,
                     Schema schema,
                     Object[] payload){
        this.timestamp = timestamp;
        this.entryType = entryType;
        this.queue = queue;
        this.schema = schema;
        this.payload = payload;
    }

    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;

    /**
     * Type of the data entry.
     */
    private EntryType entryType;

    /**
     * Related queue.
     */
    private OMSQueue queue;

    /**
     * Schema of the data entry.
     */
    private Schema schema;

    /**
     * Payload of the data entry.
     */
    private Object[] payload;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public void setEntryType(EntryType entryType) {
        this.entryType = entryType;
    }

    public OMSQueue getQueue() {
        return queue;
    }

    public void setQueue(OMSQueue queue) {
        this.queue = queue;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Object[] getPayload() {
        return payload;
    }

    public void setPayload(Object[] payload) {
        this.payload = payload;
    }
}
