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
 * Define the field type.
 *
 * @version OMS 0.1.0
 * @since OMS 0.1.0
 */
public enum FieldType {

    /**
     * Integer
     */
    INT32,

    /**
     * Long
     */
    INT64,

    /**
     * BigInteger
     */
    BIG_INTEGER,

    /**
     * Float
     */
    FLOAT32,

    /**
     * Double
     */
    FLOAT64,

    /**
     * Boolean
     */
    BOOLEAN,

    /**
     * String
     */
    STRING,

    /**
     * Byte
     */
    BYTES,

    /**
     * List
     */
    ARRAY,

    /**
     * Map
     */
    MAP,

    /**
     * Date
     */
    DATETIME;
}
