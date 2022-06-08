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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * decimal
 */
public class Decimal {
    public static final String LOGICAL_NAME = "io.openmessaging.connector.api.data.logical.Decimal";
    public static final String SCALE_FIELD = "scale";

    /**
     *schema builder
     * @param scale
     * @return
     */
    public static SchemaBuilder builder(int scale) {
        return SchemaBuilder.bytes()
                .name(LOGICAL_NAME)
                .parameter(SCALE_FIELD, Integer.toString(scale))
                .version(1);
    }

    /**
     * get schema
     * @param scale
     * @return
     */
    public static Schema schema(int scale){
        return builder(scale).build();
    }

    /**
     * Convert a value from its logical format (BigDecimal) to it's encoded format.
     * @param schema
     * @param value
     * @return
     */
    public static byte[] fromLogical(Schema schema, BigDecimal value) {
        if (value.scale() != scale(schema)) {
            throw new ConnectException("BigDecimal has mismatching scale value for given Decimal schema");
        }
        return value.unscaledValue().toByteArray();
    }

    public static BigDecimal toLogical(Schema schema, byte[] value) {
        return new BigDecimal(new BigInteger(value), scale(schema));
    }

    private static int scale(Schema schema) {
        String scaleString = schema.getParameters().get(SCALE_FIELD);
        if (scaleString == null) {
            throw new ConnectException("Invalid Decimal schema: scale parameter not found.");
        }
        try {
            return Integer.parseInt(scaleString);
        } catch (NumberFormatException e) {
            throw new ConnectException("Invalid scale parameter found in Decimal schema: ", e);
        }
    }
}
