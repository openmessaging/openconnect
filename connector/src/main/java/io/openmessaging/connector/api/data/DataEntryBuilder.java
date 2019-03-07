package io.openmessaging.connector.api.data;

import java.nio.ByteBuffer;

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
     * Related queue name.
     */
    private String queueName;

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

    public DataEntryBuilder queue(String queueName){
        this.queueName = queueName;
        return this;
    }

    public DataEntryBuilder putFiled(String fieldName, Object value){

        Field field = lookupField(fieldName);
        payload[field.getIndex()] = value;
        return this;
    }

    public SourceDataEntry buildSourceDataEntry(ByteBuffer sourcePartition, ByteBuffer sourcePosition){

        return new SourceDataEntry(sourcePartition, sourcePosition, timestamp, entryType, queueName, schema, payload);
    }

    public SinkDataEntry buildSinkDataEntry(Long queueOffset){

        return new SinkDataEntry(queueOffset, timestamp, entryType, queueName, schema, payload);
    }

    private Field lookupField(String fieldName) {
        Field field = schema.getField(fieldName);
        if (field == null)
            throw new RuntimeException(fieldName + " is not a valid field name");
        return field;
    }
}
