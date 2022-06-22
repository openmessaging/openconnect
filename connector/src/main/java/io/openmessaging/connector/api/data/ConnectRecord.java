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

package io.openmessaging.connector.api.data;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.Objects;
import java.util.Set;

/**
 * SourceDataEntries are generated by SourceTasks and passed to specific message queue to store.
 */
public abstract class ConnectRecord<R extends ConnectRecord<R>> {
    /**
     *  Topic of the data entry.
     */
    private String topic;

    /**
     * queueId of the entry
     */
    private Integer queueId;

    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;

    /**
     * Schema of the data entry.
     */
    private Schema schema;

    /**
     * Payload of the data entry.
     */
    private Object data;

    /**
     * Extension properties
     */
    private KeyValue extensions;

    public ConnectRecord(String topic, Integer queueId, Long timestamp, Schema schema, Object data) {
        this(topic, queueId, timestamp, schema, data, null);
    }

    public ConnectRecord(String topic, Integer queueId, Long timestamp, Schema schema, Object data,KeyValue extensions) {
        this.topic = topic;
        this.queueId = queueId;
        this.schema = schema;
        this.timestamp = timestamp;
        this.data = data;
        this.extensions = extensions;
    }


    /**
     * new record
     * @param topic
     * @param queueId
     * @param schema
     * @param data
     * @param timestamp
     * @return
     */
    public abstract R newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp);

    /**
     *  new record
     * @param topic
     * @param queueId
     * @param schema
     * @param data
     * @param timestamp
     * @param extensions
     * @return
     */
    public abstract R newRecord(String topic, Integer queueId, Schema schema, Object data, Long timestamp, KeyValue extensions);


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public KeyValue getExtensions() {
        return extensions;
    }

    public void setExtensions(KeyValue extensions) {
        this.extensions = extensions;
    }

    public void addExtension(KeyValue extensions) {
        if (this.extensions == null) {
            this.extensions = new DefaultKeyValue();
        }
        Set<String> keySet = extensions.keySet();
        for (String key : keySet) {
            this.extensions.put(key, extensions.getString(key));
        }
    }

    public void addExtension(String key, String value) {
        if (this.extensions == null) {
            this.extensions = new DefaultKeyValue();
        }
        this.extensions.put(key, value);
    }

    public String getExtension(String key) {
        if (this.extensions == null) {
            return null;
        }
        return this.extensions.getString(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnectRecord)) return false;
        ConnectRecord record = (ConnectRecord) o;
        return topic.equals(record.topic) && queueId.equals(record.queueId) && getTimestamp().equals(record.getTimestamp()) && getSchema().equals(record.getSchema()) && getData().equals(record.getData()) && getExtensions().equals(record.getExtensions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, queueId, getTimestamp(), getSchema(), getData(), getExtensions());
    }

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", timestamp=" + timestamp +
                ", schema=" + schema +
                ", data=" + data +
                ", extensions=" + extensions +
                '}';
    }
}
