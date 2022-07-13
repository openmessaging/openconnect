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

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset, String topic, Schema schema, Object data, KeyValue extensions) {
        this(recordPartition, recordOffset, topic, null,schema , data, extensions);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset,String topic, Schema schema, Object data) {
        this(recordPartition, recordOffset, topic, null, schema , data, null);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset,String topic, Long timestamp, Schema schema, Object data) {
        this(recordPartition, recordOffset, topic, timestamp, schema , data, null);
    }

    public SourceRecord(RecordPartition recordPartition, RecordOffset recordOffset, String topic, Long timestamp, Schema schema, Object data, KeyValue extensions) {
        super(topic, timestamp, schema, data, extensions);
        this.position = new RecordPosition(recordPartition, recordOffset);
    }

    public RecordPosition position() {
        return position;
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
    public SourceRecord newRecord(String topic, Schema schema, Object data, Long timestamp) {
        return newRecord(topic, schema , data, timestamp, null );
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
    public SourceRecord newRecord(String topic, Schema schema, Object data, Long timestamp, KeyValue extensions) {
        return new SourceRecord(position().getPartition(), position().getOffset(), topic, timestamp, schema, data, extensions);
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
