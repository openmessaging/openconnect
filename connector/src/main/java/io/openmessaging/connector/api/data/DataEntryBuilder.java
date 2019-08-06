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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import io.openmessaging.connector.api.header.DataHeaders;
import io.openmessaging.connector.api.header.Header;
import io.openmessaging.connector.api.header.Headers;

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
     * Related queue name.
     */
    private String queueName;

    /**
     * The queueId of queue.
     */
    private Integer queueId;

    /**
     * Type of the data entry.
     */
    private EntryType entryType;

    /**
     * Key of the data entry.
     */
    private MetaAndData key;

    /**
     * Value of the data entry.
     */
    private MetaAndData value;

    /**
     * The Headers of data.
     */
    private Headers headers;

    public DataEntryBuilder() {
        this.headers = new DataHeaders();
    }

    public DataEntryBuilder(Meta valueMeta) {
        this(null, new MetaAndData(valueMeta));
    }

    public DataEntryBuilder(MetaAndData valueMetaAndData) {
        this(null, valueMetaAndData);
    }

    public DataEntryBuilder(Meta keyMeta, Meta valueMeta) {
        this(new MetaAndData(keyMeta), new MetaAndData(valueMeta));
    }

    public DataEntryBuilder(MetaAndData keyMetaAndData, MetaAndData valueMetaAndData) {
        this.key = keyMetaAndData;
        this.value = valueMetaAndData;
        this.headers = new DataHeaders();
    }

    public static DataEntryBuilder newDataEntryBuilder(Meta keyMeta, Meta valueMeta){
        return new DataEntryBuilder(keyMeta, valueMeta);
    }

    public static DataEntryBuilder newDataEntryBuilder(Meta valueMeta){
        return new DataEntryBuilder(valueMeta);
    }

    public static DataEntryBuilder newDataEntryBuilder(){
        return new DataEntryBuilder();
    }

    public DataEntryBuilder timestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public DataEntryBuilder queueId(Integer queueId){
        this.queueId = queueId;
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

    // headers

    public DataEntryBuilder header(Header header){
        this.headers.add(header);
        return this;
    }

    public DataEntryBuilder header(String key, Meta meta, Object value){
        this.headers.add(key, meta, value);
        return this;
    }

    public DataEntryBuilder header(String key, String value){
        this.headers.addString(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, boolean value){
        this.headers.addBoolean(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, byte value){
        this.headers.addByte(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, short value){
        this.headers.addShort(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, int value){
        this.headers.addInt(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, long value){
        this.headers.addLong(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, float value){
        this.headers.addFloat(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, double value){
        this.headers.addDouble(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, byte[] value){
        this.headers.addBytes(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, List<?> value, Meta meta){
        this.headers.addList(key, value, meta);
        return this;
    }

    public DataEntryBuilder header(String key, Map<?, ?> value, Meta meta){
        this.headers.addMap(key, value, meta);
        return this;
    }

    public DataEntryBuilder header(String key, Struct value){
        this.headers.addStruct(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, BigDecimal value){
        this.headers.addDecimal(key, value);
        return this;
    }

    public DataEntryBuilder header(String key, java.util.Date value){
        this.headers.addDate(key, value);
        return this;
    }

    public DataEntryBuilder keyMeta(Meta keyMeta){
        MetaAndData tmpKey = new MetaAndData(keyMeta);
        if(this.key != null && this.key.getData() != null){
            tmpKey.putData(this.key.getData());
        }
        this.key = tmpKey;
        return this;
    }

    public DataEntryBuilder valueMeta(Meta valueMeta){
        MetaAndData tmpValue = new MetaAndData(valueMeta);
        if(this.value != null && this.value.getData() != null){
            tmpValue.putData(this.value.getData());
        }
        this.value = tmpValue;
        return this;
    }


    // base

    public DataEntryBuilder keyData(Object data){
        switch (this.key.getMeta().getType()){
            case STRUCT:
                List<Field> fields = null;
                if(data instanceof Struct){
                    fields = ((Struct)data).getFields();
                } else if(data instanceof MetaAndData){
                    fields = ((MetaAndData)data).getMeta().getFields();
                } else{
                    MetaAndData strData = MetaAndData.getMetaDataFromString(data.toString());
                    fields = strData.getMeta().getFields();
                }
                if(fields != null){
                    for (int i = 0; i < fields.size(); i++) {
                        String fieldName = fields.get(i).name();
                        keyData(fieldName, ((Struct)data).getObject(fieldName));
                    }
                }
                break;
            case MAP:
                keyData((Map)data);
                break;
            case ARRAY:
                keyData((List)data);
                break;
            default:
                this.key.putData(data);
                break;
        }
        return this;
    }

    public DataEntryBuilder valueData(Object data){
        switch (this.value.getMeta().getType()){
            case STRUCT:
                List<Field> fields = null;
                if(data instanceof Struct){
                    fields = ((Struct)data).getFields();
                } else if(data instanceof MetaAndData){
                    fields = ((MetaAndData)data).getMeta().getFields();
                } else{
                    MetaAndData strData = MetaAndData.getMetaDataFromString(data.toString());
                    fields = strData.getMeta().getFields();
                }
                if(fields != null){
                    for (int i = 0; i < fields.size(); i++) {
                        String fieldName = fields.get(i).name();
                        valueData(fieldName, ((Struct)data).getObject(fieldName));
                    }
                }
                break;
            case MAP:
                valueData((Map)data);
                break;
            case ARRAY:
                valueData((List)data);
                break;
            default:
                this.value.putData(data);
                break;
        }
        return this;
    }


    // map

    public DataEntryBuilder keyData(Object key, Object value){
        this.key.putData(key, value);
        return this;
    }

    public DataEntryBuilder keyData(Map map){
        this.key.putData(map);
        return this;
    }

    public DataEntryBuilder valueData(Object key, Object value){
        this.value.putData(key, value);
        return this;
    }

    public DataEntryBuilder valueData(Map map){
        this.value.putData(map);
        return this;
    }

    // array

    public DataEntryBuilder keyData(List elements){
        this.key.putData(elements);
        return this;
    }

    public DataEntryBuilder valueData(List elements){
        this.value.putData(elements);
        return this;
    }


    // struct

    public DataEntryBuilder keyData(String fieldName, Object value) {
        this.key.putData(fieldName, value);
        return this;
    }

    public DataEntryBuilder keyData(Field field, Object value) {
        this.key.putData(field, value);
        return this;
    }

    public DataEntryBuilder key(MetaAndData metaAndData) {
        this.key = metaAndData;
        return this;
    }


    public DataEntryBuilder valueData(String fieldName, Object data) {
        this.value.putData(fieldName, data);
        return this;
    }

    public DataEntryBuilder valueData(Field field, Object data) {
        this.value.putData(field, data);
        return this;
    }

    public DataEntryBuilder value(MetaAndData metaAndData) {
        this.value = metaAndData;
        return this;
    }



    public SourceDataEntry buildSourceDataEntry(ByteBuffer sourcePartition, ByteBuffer sourcePosition) {

        return new SourceDataEntry(sourcePartition, sourcePosition, timestamp, queueName, queueId, entryType, key,
            value, headers);
    }

    public SinkDataEntry buildSinkDataEntry(Long queueOffset) {

        return new SinkDataEntry(queueOffset, timestamp, queueName, queueId, entryType, key, value, headers);
    }


}
