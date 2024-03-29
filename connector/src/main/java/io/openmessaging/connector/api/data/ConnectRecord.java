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
public class ConnectRecord {

    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;

    /**
     * key schema
     */
    private Schema keySchema;

    /**
     * Payload of the key entry.
     */
    private Object key;

    /**
     * Schema of the data entry.
     */
    private Schema schema;

    /**
     * Payload of the data entry.
     */
    private Object data;

    /**
     * Position of record
     */
    private RecordPosition position;

    /**
     * Extension properties
     */
    private KeyValue extensions;

    public ConnectRecord(RecordPartition recordPartition, RecordOffset recordOffset,
        Long timestamp) {
        this(recordPartition, recordOffset, timestamp, null, null);
    }

    public ConnectRecord(RecordPartition recordPartition, RecordOffset recordOffset,
        Long timestamp, Schema schema,
        Object data) {
        this.position = new RecordPosition(recordPartition, recordOffset);
        this.schema = schema;
        this.timestamp = timestamp;
        this.data = data;
    }

    public ConnectRecord(RecordPartition recordPartition, RecordOffset recordOffset,
                         Long timestamp,Schema keySchema, Object key, Schema schema,
                         Object data) {
        this.position = new RecordPosition(recordPartition, recordOffset);
        this.timestamp = timestamp;

        // key
        this.keySchema = keySchema;
        this.key = key;

        // value
        this.schema = schema;
        this.data = data;

    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(Schema keySchema) {
        this.keySchema = keySchema;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
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

    public RecordPosition getPosition() {
        return position;
    }

    public void setPosition(RecordPosition position) {
        this.position = position;
    }

    /**
     * add extension by KeyValue
     * @param extensions
     */
    public void addExtension(KeyValue extensions) {
        if (this.extensions == null) {
            this.extensions = new DefaultKeyValue();
        }
        Set<String> keySet = extensions.keySet();
        for (String key : keySet) {
            this.extensions.put(key, extensions.getString(key));
        }
    }

    /**
     * add extension by key and value
     * @param key
     * @param value
     */
    public void addExtension(String key, String value) {
        if (this.extensions == null) {
            this.extensions = new DefaultKeyValue();
        }
        this.extensions.put(key, value);
    }

    /**
     * get extension value
     * @param key
     * @return
     */
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
        ConnectRecord that = (ConnectRecord) o;
        return Objects.equals(timestamp, that.timestamp) && Objects.equals(keySchema, that.keySchema) && Objects.equals(key, that.key) && Objects.equals(schema, that.schema) && Objects.equals(data, that.data) && Objects.equals(position, that.position) && Objects.equals(extensions, that.extensions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, keySchema, key, schema, data, position, extensions);
    }

    @Override
    public String toString() {
        return "ConnectRecord{" +
                "timestamp=" + timestamp +
                ", keySchema=" + keySchema +
                ", key=" + key +
                ", schema=" + schema +
                ", data=" + data +
                ", position=" + position +
                ", extensions=" + extensions +
                '}';
    }
}
