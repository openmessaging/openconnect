package io.openmessaging.connector.api.data;

import java.nio.ByteBuffer;

public class SourceDataEntry extends DataEntry {

    public SourceDataEntry(ByteBuffer sourcePartition,
                           ByteBuffer sourcePosition,
                           Long timestamp,
                           EntryType entryType,
                           String queueName,
                           Schema schema,
                           Object[] payload){
        super(timestamp, entryType, queueName, schema, payload);
        this.sourcePartition = sourcePartition;
        this.sourcePosition = sourcePosition;
    }

    /**
     * Partition of the data source.
     */
    private ByteBuffer sourcePartition;

    /**
     * position of current data entry in data source.
     */
    private ByteBuffer sourcePosition;

    public ByteBuffer getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(ByteBuffer sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public ByteBuffer getSourcePosition() {
        return sourcePosition;
    }

    public void setSourcePosition(ByteBuffer sourcePosition) {
        this.sourcePosition = sourcePosition;
    }
}
