/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.connector.api.component.task.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;

import java.util.Objects;

/**
 *  sink connect record
 */
public class SinkRecord extends ConnectRecord<SinkRecord> {

    private final Integer queueId;
    private final String brokerName;
    private final long queueOffset;

    public SinkRecord(String brokerName, long queueOffset,String topic, Integer queueId,  Schema schema, Object data) {
        this(brokerName, queueOffset, topic, queueId, null, null, schema, data ,null );
    }

    public SinkRecord(String brokerName, long queueOffset, String topic, Integer queueId, Schema keySchema, Object key, Schema schema, Object data) {
        this(brokerName, queueOffset, topic, queueId, null, keySchema, key, schema, data ,null );
    }

    public SinkRecord(String brokerName, long queueOffset, String topic, Integer queueId, Schema keySchema, Object key, Schema schema, Object data, KeyValue extensions) {
        this(brokerName, queueOffset, topic, queueId, null, keySchema, key, schema, data ,extensions );
    }

    public SinkRecord(String brokerName, long queueOffset, String topic, Integer queueId, Long timestamp, Schema keySchema, Object key, Schema schema, Object data, KeyValue extensions) {
        super(topic, timestamp, keySchema, key, schema, data, extensions);
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
     * @param schema
     * @param data
     * @param timestamp
     * @return
     */
    @Override
    public SinkRecord newRecord(String topic, Long timestamp, Schema keySchema, Object key,Schema schema, Object data) {
        return newRecord(topic, timestamp, keySchema, key, schema, data, null);
    }

    /**
     * new record
     *
     * @param topic
     * @param schema
     * @param data
     * @param timestamp
     * @param extensions
     * @return
     */
    @Override
    public SinkRecord newRecord(String topic, Long timestamp, Schema keySchema, Object key, Schema schema, Object data, KeyValue extensions) {
        return new SinkRecord(brokerName(), queueOffset(), topic, queueId(), timestamp, keySchema, key, schema, data, extensions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SinkRecord)) return false;
        if (!super.equals(o)) return false;
        SinkRecord that = (SinkRecord) o;
        return queueOffset == that.queueOffset && Objects.equals(queueId, that.queueId) && Objects.equals(brokerName, that.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queueId, brokerName, queueOffset);
    }

    @Override
    public String toString() {
        return "SinkRecord{" +
                "queueId=" + queueId +
                ", brokerName='" + brokerName + '\'' +
                ", queueOffset=" + queueOffset +
                "} " + super.toString();
    }
}
