/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.openmessaging.connector.api.data;

import java.util.Arrays;
import java.util.Objects;

/**
 * Base class for records containing data to be copied to/from message queue.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class DataEntry {

    public DataEntry(Long timestamp,
        EntryType entryType,
        String queueName,
        Schema schema,
        Object[] payload) {
        this(timestamp, entryType, queueName, schema, null, payload);
    }

    public DataEntry(Long timestamp,
        EntryType entryType,
        String queueName,
        Schema schema,
        String shardingKey,
        Object[] payload) {
        this.timestamp = timestamp;
        this.entryType = entryType;
        this.queueName = queueName;
        this.schema = schema;
        this.shardingKey = shardingKey;
        this.payload = payload;
    }

    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;

    /**
     * Type of the data entry.
     */
    private EntryType entryType;

    /**
     * Related queueName.
     */
    private String queueName;

    /**
     * Used for shard to related queue/partition.
     */
    private String shardingKey;

    /**
     * Schema of the data entry.
     */
    private Schema schema;

    /**
     * Payload of the data entry.
     */
    private Object[] payload;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public void setEntryType(EntryType entryType) {
        this.entryType = entryType;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Object[] getPayload() {
        return payload;
    }

    public void setPayload(Object[] payload) {
        this.payload = payload;
    }

    public String getShardingKey() {
        return shardingKey;
    }

    public void setShardingKey(String shardingKey) {
        this.shardingKey = shardingKey;
    }

    @Override public String toString() {
        return "DataEntry{" +
            "timestamp=" + timestamp +
            ", entryType=" + entryType +
            ", queueName='" + queueName + '\'' +
            ", shardingKey='" + shardingKey + '\'' +
            ", schema=" + schema +
            ", payload=" + Arrays.toString(payload) +
            '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DataEntry))
            return false;
        DataEntry entry = (DataEntry) o;
        return Objects.equals(timestamp, entry.timestamp) &&
            entryType == entry.entryType &&
            Objects.equals(queueName, entry.queueName) &&
            Objects.equals(shardingKey, entry.shardingKey) &&
            Objects.equals(schema, entry.schema) &&
            Arrays.equals(payload, entry.payload);
    }

    @Override public int hashCode() {
        int result = Objects.hash(timestamp, entryType, queueName, shardingKey, schema);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
