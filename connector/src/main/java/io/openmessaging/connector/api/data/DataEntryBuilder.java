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

/**
 * Use DataEntryBuilder to build SourceDataEntry or SinkDataEntry.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public class DataEntryBuilder {

    /**
     * Timestamp of the data entry.
     */
    private Long timestamp;

    /**
     * Type of the data entry.
     */
    private EntryType entryType;

    /**
     * Related queue name.
     */
    private String queueName;

    /**
     * Schema of the data entry.
     */
    private Schema schema;

    /**
     * Payload of the data entry.
     */
    private Object[] payload;

    public DataEntryBuilder(Schema schema) {
        this.schema = schema;
        this.payload = new Object[schema.getFields().size()];
    }

    public DataEntryBuilder timestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public DataEntryBuilder entryType(EntryType entryType) {
        this.entryType = entryType;
        return this;
    }

    public DataEntryBuilder queue(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public DataEntryBuilder putFiled(String fieldName, Object value) {

        Field field = lookupField(fieldName);
        payload[field.getIndex()] = value;
        return this;
    }

    public SourceDataEntry buildSourceDataEntry(byte[] sourcePartition, byte[] sourcePosition) {

        return new SourceDataEntry(sourcePartition, sourcePosition, timestamp, entryType, queueName, schema, payload);
    }

    public SinkDataEntry buildSinkDataEntry(Long queueOffset) {

        return new SinkDataEntry(queueOffset, timestamp, entryType, queueName, schema, payload);
    }

    private Field lookupField(String fieldName) {
        Field field = schema.getField(fieldName);
        if (field == null)
            throw new RuntimeException(fieldName + " is not a valid field name");
        return field;
    }
}
