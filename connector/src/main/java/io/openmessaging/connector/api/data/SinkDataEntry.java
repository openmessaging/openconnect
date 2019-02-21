package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.sink.OMSQueue;

public class SinkDataEntry extends DataEntry {

    public SinkDataEntry(Long queueOffset,
                         Long timestamp,
                         EntryType entryType,
                         OMSQueue queue,
                         Schema schema,
                         Object[] payload){
        super(timestamp, entryType, queue, schema, payload);
        this.queueOffset = queueOffset;
    }

    /**
     * offset in the queue.
     */
    private Long queueOffset;

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }
}
