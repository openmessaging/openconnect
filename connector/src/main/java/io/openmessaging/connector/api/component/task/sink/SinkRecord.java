package io.openmessaging.connector.api.component.task.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;

import java.util.Objects;

/**
 *  sink connect record
 */
public class SinkRecord extends ConnectRecord<SinkRecord> {

    private Integer queueId;
    private final String brokerName;
    private final long queueOffset;

    public SinkRecord(String brokerName, long queueOffset,String topic, Integer queueId,  Schema schema, Object data) {
        this(brokerName, queueOffset, topic, queueId, null, schema, data ,null );
    }

    public SinkRecord(String brokerName, long queueOffset,String topic, Integer queueId,  Schema schema, Object data, KeyValue extensions) {
        this(brokerName, queueOffset, topic, queueId, null, schema, data ,extensions );
    }

    public SinkRecord(String brokerName, long queueOffset,String topic, Integer queueId, Long timestamp, Schema schema, Object data) {
        this(brokerName, queueOffset, topic, queueId, timestamp, schema, data ,null );
    }

    public SinkRecord(String brokerName, long queueOffset, String topic, Integer queueId, Long timestamp, Schema schema, Object data, KeyValue extensions) {
        super(topic, timestamp, schema, data, extensions);
        this.brokerName = brokerName;
        this.queueOffset = queueOffset;
        this.queueId = queueId;
    }


    public Integer queueId(){
        return queueId;
    }

    public String brokerName(){
        return brokerName;
    }

    public long queueOffset(){
        return queueOffset;
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
    public SinkRecord newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp) {
        return newRecord(topic,queueId,schema,data,timestamp, null);
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
    public SinkRecord newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp, KeyValue extensions) {
        return new SinkRecord(brokerName(), queueOffset(), topic, queueId, timestamp, schema, data, extensions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SinkRecord)) return false;
        if (!super.equals(o)) return false;
        SinkRecord that = (SinkRecord) o;
        return queueOffset == that.queueOffset && Objects.equals(brokerName, that.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), brokerName, queueOffset);
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "brokerName='" + brokerName + '\'' +
                ", queueOffset=" + queueOffset +
                "} " + super.toString();
    }
}
