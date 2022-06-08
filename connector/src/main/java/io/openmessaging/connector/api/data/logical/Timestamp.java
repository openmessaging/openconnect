/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.connector.api.data.logical;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.errors.ConnectException;

/**
 *  A timestamp representing an absolute time, without timezone information. The corresponding Java type is a
 *  java.util.Date. The underlying representation is a long representing the number of milliseconds since Unix epoch.
 */
public class Timestamp {
    public static final String LOGICAL_NAME = "io.openmessaging.connector.api.data.logical.Timestamp";


    /**
     * build schema
     * @return
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int64()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().build();

    /**
     * Convert a value from its logical format (Date) to it's encoded format.
     */
    public static long fromLogical(Schema schema, java.util.Date value) {
        if (!(LOGICAL_NAME.equals(schema.getName()))) {
            throw new ConnectException("Requested conversion of Timestamp object but the schema does not match.");
        }
        return value.getTime();
    }

    public static java.util.Date toLogical(Schema schema, long value) {
        if (!(LOGICAL_NAME.equals(schema.getName()))) {
            throw new ConnectException("Requested conversion of Timestamp object but the schema does not match.");
        }
        return new java.util.Date(value);
    }
}
