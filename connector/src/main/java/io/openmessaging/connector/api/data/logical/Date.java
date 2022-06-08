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

import java.util.Calendar;
import java.util.TimeZone;

/**
 * A date representing a calendar day with no time of day or timezone.
 **/
public class Date {
    public static final String LOGICAL_NAME = "io.openmessaging.connector.api.data.logical.Date";

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");


    /**
     * build schema
     * @return
     */
    public static SchemaBuilder builder() {
        return SchemaBuilder.int32()
                .name(LOGICAL_NAME)
                .version(1);
    }

    public static final Schema SCHEMA = builder().build();

    /**
     * Convert a value from its logical format (Date) to it's encoded format.
     * @param value the logical value
     * @return the encoded value
     */
    public static int fromLogical(Schema schema, java.util.Date value) {
        if (!(LOGICAL_NAME.equals(schema.getName()))) {
            throw new ConnectException("Requested conversion of Date object but the schema does not match.");
        }
        Calendar calendar = Calendar.getInstance(UTC);
        calendar.setTime(value);
        if (calendar.get(Calendar.HOUR_OF_DAY) != 0 || calendar.get(Calendar.MINUTE) != 0 ||
                calendar.get(Calendar.SECOND) != 0 || calendar.get(Calendar.MILLISECOND) != 0) {
            throw new ConnectException("Kafka Connect Date type should not have any time fields set to non-zero values.");
        }
        long unixMillis = calendar.getTimeInMillis();
        return (int) (unixMillis / MILLIS_PER_DAY);
    }

    public static java.util.Date toLogical(Schema schema, int value) {
        if (!(LOGICAL_NAME.equals(schema.getName()))) {
            throw new ConnectException("Requested conversion of Date object but the schema does not match.");
        }
        return new java.util.Date(value * MILLIS_PER_DAY);
    }
}
