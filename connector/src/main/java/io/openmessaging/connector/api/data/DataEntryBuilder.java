package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.sink.OMSQueue;

/**
 * Use DataEntryBuilder to build SourceDataEntry or SinkDataEntry.
 */
public class DataEntryBuilder {

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

    public DataEntryBuilder(Schema schema){
        this.schema = schema;
        this.payload = new Object[schema.getFields().size()];
    }

    public DataEntryBuilder timestamp(Long timestamp){
        this.timestamp = timestamp;
        return this;
    }

    public DataEntryBuilder entryType(EntryType entryType){
        this.entryType = entryType;
        return this;
    }

    public DataEntryBuilder queue(OMSQueue queue){
        this.queue = queue;
        return this;
    }

    public DataEntryBuilder putFiled(String fieldName, Object value){

        Field field = lookupField(fieldName);
        payload[field.getIndex()] = value;
        return this;
    }

    public SourceDataEntry buildSourceDataEntry(byte[] sourcePartition, byte[] sourcePosition){

        return new SourceDataEntry(sourcePartition, sourcePosition, timestamp, entryType, queue, schema, payload);
    }

    public SinkDataEntry buildSinkDataEntry(Long queueOffset){

        return new SinkDataEntry(queueOffset, timestamp, entryType, queue, schema, payload);
    }

    private Field lookupField(String fieldName) {
        Field field = schema.getField(fieldName);
        if (field == null)
            throw new RuntimeException(fieldName + " is not a valid field name");
        return field;
    }
}
