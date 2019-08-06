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

import java.util.Objects;

import io.openmessaging.connector.api.header.DataHeaders;
import io.openmessaging.connector.api.header.Header;
import io.openmessaging.connector.api.header.Headers;

/**
 * Base class for records containing data to be copied to/from message queue.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public abstract class DataEntry {
    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;
    /**
     * Related queueName.
     */
    private String queueName;
    /**
     * The id of queue.
     */
    private Integer queueId;
    /**
     * {@link EntryType} of the {@link DataEntry}
     */
    private EntryType entryType;
    /**
     * Definition data key.
     */
    private MetaAndData key;
    /**
     * Definition data value.
     */
    private MetaAndData value;
    /**
     * The Headers of data.
     */
    private Headers headers;


    public DataEntry(Long timestamp,
        String queueName,
        Integer queueId,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value) {
        this(timestamp, queueName, queueId, entryType, key, value, new DataHeaders());
    }

    public DataEntry(Long timestamp,
        String queueName,
        Integer queueId,
        EntryType entryType,
        MetaAndData key,
        MetaAndData value,
        Iterable<Header> headers) {
        this.timestamp = timestamp;
        this.queueName = queueName;
        this.queueId = queueId;
        this.entryType = entryType;
        this.key = key;
        this.value = value;
        if (headers instanceof DataHeaders) {
            this.headers = (DataHeaders) headers;
        } else {
            this.headers = new DataHeaders(headers);
        }
    }

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

    public MetaAndData getKey() {
        return key;
    }

    public void setKey(MetaAndData meta) {
        this.key = meta;
    }

    public MetaAndData getValue() {
        return value;
    }

    public void setValue(MetaAndData value) {
        this.value = value;
    }

    public Headers getHeaders() {
        return headers;
    }

    public void setHeaders(Headers headers) {
        this.headers = headers;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }


    @Override
    public String toString() {
        return "DataEntry{" +
            "queueName='" + this.queueName + '\'' +
            ", queueId='" + this.queueId +
            ", entryType='" + this.entryType + '\'' +
            ", key='" + this.key + '\'' +
            ", value='" + this.value + '\'' +
            ", timestamp='" + timestamp + '\'' +
            ", headers'=" + headers + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataEntry that = (DataEntry) o;

        return Objects.equals(this.queueId, that.queueId)
            && Objects.equals(this.queueName, that.queueName)
            && Objects.equals(this.entryType, that.entryType)
            && Objects.equals(this.key, that.key)
            && Objects.equals(this.value, that.value)
            && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode() {
        int result = this.queueName != null ? this.queueName.hashCode() : 0;
        result = 31 * result + (this.queueId != null ? this.queueId.hashCode() : 0);
        result = 31 * result + (this.entryType != null ? entryType.hashCode() : 0);
        result = 31 * result + (this.key != null ? this.key.hashCode() : 0);
        result = 31 * result + (this.value != null ? this.value.hashCode() : 0);
        result = 31 * result + (this.timestamp != null ? this.timestamp.hashCode() : 0);
        result = 31 * result + this.headers.hashCode();
        return result;
    }
}
