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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.openmessaging.connector.api.data;

import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.List;

public class SchemaBuilder {

    /**
     * Name of the schema.
     */
    private String name;

    /**
     * Type of the field
     */
    private FieldType type;
    /**
     * Structure of the schema, contains a list of {@link Field}
     */
    private List<Field> fields;

    public SchemaBuilder(FieldType type) {
        if (null == type) {
            throw new ConnectException("type cannot be null");
        }
        this.type = type;
        if (type == FieldType.STRUCT) {
            fields = new ArrayList<>();
        }
    }

    /**
     * @return a new {@link FieldType#INT8} SchemaBuilder
     */
    public static SchemaBuilder int8() {
        return new SchemaBuilder(FieldType.INT8);
    }

    /**
     * @return a new {@link FieldType#INT32} SchemaBuilder
     */
    public static SchemaBuilder int32() {
        return new SchemaBuilder(FieldType.INT32);
    }

    /**
     * @return a new {@link FieldType#INT64} SchemaBuilder
     */
    public static SchemaBuilder int64() {
        return new SchemaBuilder(FieldType.INT64);
    }

    /**
     * @return a new {@link FieldType#FLOAT32} SchemaBuilder
     */
    public static SchemaBuilder float32() {
        return new SchemaBuilder(FieldType.FLOAT32);
    }

    /**
     * @return a new {@link FieldType#FLOAT64} SchemaBuilder
     */
    public static SchemaBuilder float64() {
        return new SchemaBuilder(FieldType.FLOAT64);
    }

    /**
     * @return a new {@link FieldType#BOOLEAN} SchemaBuilder
     */
    public static SchemaBuilder bool() {
        return new SchemaBuilder(FieldType.BOOLEAN);
    }

    /**
     * @return a new {@link FieldType#STRING} SchemaBuilder
     */
    public static SchemaBuilder string() {
        return new SchemaBuilder(FieldType.STRING);
    }

    /**
     * @return a new {@link FieldType#BYTES} SchemaBuilder
     */
    public static SchemaBuilder bytes() {
        return new SchemaBuilder(FieldType.BYTES);
    }

    // Structs

    /**
     * @return a new {@link FieldType#STRUCT} SchemaBuilder
     */
    public static SchemaBuilder struct() {
        return new SchemaBuilder(FieldType.STRUCT);
    }

    /**
     * Sets the schema name
     *
     * @param name schema name
     *
     * @return the current SchemaBuilder instance
     */
    public SchemaBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Build the Schema using the current settings
     *
     * @return the {@link Schema}
     */
    public Schema build() {
        return new Schema(name, type, fields);
    }
}
