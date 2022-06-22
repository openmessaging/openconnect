package io.openmessaging.connector.api.component.task.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import io.openmessaging.connector.api.data.Schema;

import java.util.Objects;

/**
 * source connect record
 */
public class SourceRecord extends ConnectRecord<SourceRecord> {

    private final RecordPosition position;

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset, String topic, Schema schema, Object data) {
        this(recordPartition, recordOffset, topic, null , null,schema , data, null);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset, String topic, Schema schema, Object data, KeyValue extensions) {
        this(recordPartition, recordOffset, topic, null , null,schema , data, extensions);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset,String topic, Integer queueId, Schema schema, Object data) {
        this(recordPartition, recordOffset, topic, queueId, null, schema , data, null);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset,String topic, Integer queueId, Schema schema, Object data, KeyValue extensions) {
        this(recordPartition, recordOffset, topic, queueId, null, schema , data, extensions);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset,String topic, Integer queueId, Long timestamp, Schema schema, Object data) {
        this(recordPartition, recordOffset, topic, queueId, timestamp, schema , data, null);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset, String topic, Integer queueId, Long timestamp, Schema schema, Object data, KeyValue extensions) {
        super(topic, queueId, timestamp, schema, data, extensions);
        this.position = new RecordPosition(recordPartition, recordOffset);
    }

    public RecordPosition position() {
        return position;
    }

    /**
     * new record
     *
     * @param topic
     * @param queueId
     * @param schema
     * @param data
     * @param timestamp
     * @return
     */
    @Override
    public SourceRecord newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp) {
        return newRecord(topic, queueId, schema , data, timestamp, null );
    }

    /**
     * new record
     *
     * @param topic
     * @param queueId
     * @param schema
     * @param data
     * @param timestamp
     * @param extensions
     * @return
     */
    @Override
    public SourceRecord newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp, KeyValue extensions) {
        return new SourceRecord(position().getPartition(), position().getOffset(), topic, queueId, timestamp, schema, data, extensions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SourceRecord)) return false;
        if (!super.equals(o)) return false;
        SourceRecord that = (SourceRecord) o;
        return Objects.equals(position, that.position);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), position);
    }

    @Override
    public String toString() {
        return "SourceRecord{" +
                "position=" + position +
                "} " + super.toString();
    }
}
