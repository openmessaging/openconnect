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

import io.openmessaging.connector.api.header.Headers;

/**
 * SinkDataEntry is read from message queue and includes the queueOffset of the data in message queue.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public class SinkDataEntry extends DataEntry {
    /**
     * Offset in the message queue.
     */
    private Long queueOffset;

    public SinkDataEntry(Long queueOffset,
        Long timestamp,
        String queueName,
        String shardingKey,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value,
        Headers headers) {
        super(timestamp, queueName, shardingKey, entryType, key, value, headers);
        this.queueOffset = queueOffset;
    }

    public SinkDataEntry(Long queueOffset,
        String queueName,
        String shardingKey,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value,
        Headers headers) {
        this(queueOffset, null, queueName, shardingKey, entryType, key, value, headers);
    }

    public SinkDataEntry(Long queueOffset,
        Long timestamp,
        String queueName,
        String shardingKey,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value) {
        this(queueOffset, timestamp, queueName, shardingKey, entryType, key, value, null);
    }

    public SinkDataEntry newRecord(Long queueOffset,
        Long timestamp,
        String queueName,
        String shardingKey,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value) {
        return new SinkDataEntry(queueOffset, timestamp, queueName, shardingKey, entryType, key, value,
            getHeaders().duplicate());
    }

    public SinkDataEntry newRecord(Long queueOffset,
        Long timestamp,
        String queueName,
        String shardingKey,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value,
        Headers headers) {
        return new SinkDataEntry(queueOffset, timestamp, queueName, shardingKey, entryType, key, value, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        SinkDataEntry that = (SinkDataEntry)o;

        return queueOffset.equals(that.queueOffset);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Long.hashCode(queueOffset);
        return result;
    }

    @Override
    public String toString() {
        return "SinkDataEntry{" +
            "queueOffset=" + queueOffset +
            "} " + super.toString();
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }
}
