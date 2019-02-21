package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.sink.OMSQueue;

public class SourceDataEntry extends DataEntry {

    public SourceDataEntry(byte[] sourcePartition,
                           byte[] sourcePosition,
                           Long timestamp,
                           EntryType entryType,
                           OMSQueue queue,
                           Schema schema,
                           Object[] payload){
        super(timestamp, entryType, queue, schema, payload);
        this.sourcePartition = sourcePartition;
        this.sourcePosition = sourcePosition;
    }

    /**
     * Partition of the data source.
     */
    private byte[] sourcePartition;

    /**
     * position of current data entry in data source.
     */
    private byte[] sourcePosition;

    public byte[] getSourcePartition() {
        return sourcePartition;
    }

    public void setSourcePartition(byte[] sourcePartition) {
        this.sourcePartition = sourcePartition;
    }

    public byte[] getSourcePosition() {
        return sourcePosition;
    }

    public void setSourcePosition(byte[] sourcePosition) {
        this.sourcePosition = sourcePosition;
    }
}
